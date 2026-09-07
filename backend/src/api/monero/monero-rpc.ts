import axios, { AxiosInstance, AxiosRequestConfig, AxiosResponse } from 'axios';
import { IMoneroApi, MoneroDaemonConfig, MoneroRpcError } from './monero-api.interface';

const RPC_RETRIES = Math.max(0, Number(process.env.MONEROD_RPC_RETRIES ?? 2));
const RPC_RETRY_BACKOFF_MS = Math.max(0, Number(process.env.MONEROD_RPC_RETRY_BACKOFF_MS ?? 500));
// MONEROD_RPC_TRACE=1 logs one line per daemon round trip (host, method, ms, outcome).
const RPC_TRACE = ['1', 'true', 'yes', 'on'].includes(String(process.env.MONEROD_RPC_TRACE ?? '').toLowerCase());
/** Health probes never wait longer than this, whatever the request timeout is. */
const PROBE_TIMEOUT_CAP_MS = 3_000;

export interface MoneroRpcOptions {
  /**
   * Max retries for transient errors (429 / 5xx / network). Defaults to
   * MONEROD_RPC_RETRIES. A pool with alternative nodes sets this low: a
   * second node is a better bet than a third attempt at the same one.
   */
  retries?: number;
  /**
   * When true, a timed-out request is NOT retried here; the caller (the
   * pool) fails over to another node instead. Timeouts are the expensive
   * failure — each attempt costs the full timeout — so only a single-node
   * setup with nowhere else to go should ever retry them.
   */
  failFastOnTimeout?: boolean;
}

/**
 * Thin transport for the monerod daemon. Two flavours of endpoint:
 *
 *   - JSON-RPC 2.0 at `POST /json_rpc` — `get_info`, `get_block_count`,
 *     `get_block`, `get_block_header_by_*`, `get_fee_estimate`, etc.
 *   - Plain JSON POST at `POST /<method>` — `get_transaction_pool`,
 *     `get_transactions`, `get_outs`, `is_key_image_spent`. These do NOT
 *     wrap responses in a `result` envelope.
 *
 * monerod accepts digest auth (when `--rpc-login` is set) but most public
 * nodes (cakewallet, xmr.node.live, etc.) are open. We support both via
 * axios's built-in `auth` option.
 */
export class MoneroRpc {
  private client: AxiosInstance;
  public readonly rpcUrl: string;
  private readonly traceHost: string;
  private readonly retries: number;
  private readonly failFastOnTimeout: boolean;

  constructor(private config: MoneroDaemonConfig, options: MoneroRpcOptions = {}) {
    this.rpcUrl = config.rpcUrl.replace(/\/$/, '');
    try { this.traceHost = new URL(this.rpcUrl).host; } catch { this.traceHost = this.rpcUrl; }
    this.retries = Math.max(0, options.retries ?? RPC_RETRIES);
    this.failFastOnTimeout = options.failFastOnTimeout ?? false;
    this.client = axios.create({
      baseURL: this.rpcUrl,
      timeout: config.timeoutMs,
      headers: { 'Content-Type': 'application/json' },
      auth: config.rpcUser && config.rpcPassword
        ? { username: config.rpcUser, password: config.rpcPassword }
        : undefined,
    });
  }

  /** Issue a JSON-RPC 2.0 call against `/json_rpc`. */
  public async jsonRpc<T>(method: string, params: Record<string, unknown> = {}): Promise<T> {
    const body = { jsonrpc: '2.0', id: '0', method, params };
    const { data } = await this.postWithRetry<{ result?: T; error?: MoneroRpcError }>('/json_rpc', body);
    if (data.error) {
      throw new Error(`monerod RPC error (${method}) ${data.error.code}: ${data.error.message}`);
    }
    if (data.result === undefined) {
      throw new Error(`monerod RPC ${method} returned no result`);
    }
    return data.result;
  }

  /**
   * Issue a request against a non-JSON-RPC endpoint (e.g. `/get_transaction_pool`).
   * The daemon responds with the bare JSON object — no `result` wrapper.
   */
  public async raw<T>(path: string, body: Record<string, unknown> = {}): Promise<T> {
    const normalized = path.startsWith('/') ? path : `/${path}`;
    const { data } = await this.postWithRetry<T>(normalized, body);
    return data;
  }

  /**
   * Proxy a public binary daemon endpoint. Monero wallet2 uses a few
   * portable-binary daemon calls for scanning; this keeps the transport
   * generic while the route layer owns the public-method whitelist.
   */
  public async rawBytes(path: string, body: Buffer | Uint8Array): Promise<{ data: Buffer; contentType: string }> {
    const normalized = path.startsWith('/') ? path : `/${path}`;
    const { data, headers } = await this.postWithRetry<ArrayBuffer>(normalized, body, {
      headers: { 'Content-Type': 'application/octet-stream' },
      responseType: 'arraybuffer',
    });
    return {
      data: Buffer.from(data),
      contentType: String(headers['content-type'] || 'application/octet-stream'),
    };
  }

  private async postWithRetry<T>(
    path: string,
    body: unknown,
    requestConfig?: AxiosRequestConfig,
  ): Promise<AxiosResponse<T>> {
    let lastError: unknown;
    const label = path === '/json_rpc' && body && typeof body === 'object' && 'method' in (body as Record<string, unknown>)
      ? `json_rpc:${String((body as Record<string, unknown>).method)}`
      : path;
    for (let attempt = 0; attempt <= this.retries; attempt++) {
      const startedAt = Date.now();
      try {
        const res = await this.client.post<T>(path, body, requestConfig);
        if (RPC_TRACE) {
          // eslint-disable-next-line no-console
          console.log(`[xmr-space] rpc ${this.traceHost} ${label} ${Date.now() - startedAt}ms ok attempt=${attempt}`);
        }
        return res;
      } catch (err) {
        lastError = err;
        if (RPC_TRACE) {
          // eslint-disable-next-line no-console
          console.log(`[xmr-space] rpc ${this.traceHost} ${label} ${Date.now() - startedAt}ms FAIL attempt=${attempt} ${formatError(err)}`);
        }
        if (attempt >= this.retries || !isTransientRpcError(err)) {
          throw err;
        }
        if (this.failFastOnTimeout && isTimeoutError(err)) {
          throw err;
        }
        await sleep(RPC_RETRY_BACKOFF_MS * (attempt + 1));
      }
    }
    throw lastError;
  }
}

/**
 * Sync-aware primary/fallback transport. The primary is normally the local
 * monerod (or a verifying proxy such as mnr.network); the fallbacks are
 * public daemons used while the primary is syncing, down, or refusing a
 * particular endpoint.
 *
 * Design rules, all learned the hard way:
 *   - No user request ever waits on a health probe. Health state is
 *     stale-while-revalidate: reads use the last known state and a probe
 *     runs in the background once the state is older than the interval.
 *   - A timed-out node is not retried; the request fails over instead.
 *     Retrying a hung node three times turned a 10s timeout into a 45s
 *     stall on every dashboard load.
 *   - Per-path routing has memory. Endpoints the primary does not serve
 *     (`/get_transaction_pool`) go to the fallbacks first, but the node
 *     that last answered is tried first next time, and a node that failed
 *     is skipped for the health-check interval.
 */
export class MoneroRpcPool {
  private primary: MoneroRpc;
  private primaryProbe: MoneroRpc;
  private fallbacks: MoneroRpc[];
  private primaryUsable: boolean | null = null;
  private primaryCheckedAt = 0;
  private probeInflight: Promise<boolean> | null = null;
  private lastWarning = '';
  private lastWarningAt = 0;
  /** Sticky routing for PRIMARY_SKIP_PATHS: path -> node that last served it. */
  private pathPreferred = new Map<string, MoneroRpc>();
  /** `${node}|${path}` -> epoch ms until which that node is skipped for that path. */
  private pathBadUntil = new Map<string, number>();
  /** Fallback node url -> epoch ms until which it is skipped on the general path. */
  private fallbackBadUntil = new Map<string, number>();

  // monerod only serves the full mempool dump (/get_transaction_pool) in
  // unrestricted mode, so a restricted public node or verifying proxy may
  // 403 it (mnr.network's free tier does; Pro serves it). Prefer the
  // fallbacks for this one endpoint — a local node is the cheap source and
  // the data is unverifiable anyway — but keep the primary as a candidate
  // and remember which node actually answers.
  private static readonly PRIMARY_SKIP_PATHS = new Set([
    '/get_transaction_pool',
  ]);

  constructor(private config: MoneroDaemonConfig) {
    const fallbackUrls = (config.fallbackRpcUrls ?? []).filter((url) => url.trim().length > 0);
    const hasFallback = fallbackUrls.length > 0;
    const primaryTimeoutMs = Math.max(500, config.primaryTimeoutMs ?? config.timeoutMs);
    // With somewhere else to go, one retry on 429/5xx is plenty and a
    // timeout is not retried at all. Single-node keeps the env defaults.
    const nodeOptions: MoneroRpcOptions = hasFallback
      ? { retries: Math.min(1, RPC_RETRIES), failFastOnTimeout: true }
      : {};
    this.primary = new MoneroRpc({ ...config, timeoutMs: primaryTimeoutMs }, nodeOptions);
    this.primaryProbe = new MoneroRpc(
      { ...config, timeoutMs: Math.min(primaryTimeoutMs, PROBE_TIMEOUT_CAP_MS) },
      { retries: 0, failFastOnTimeout: true },
    );
    this.fallbacks = fallbackUrls.map((rpcUrl) => new MoneroRpc({
      ...config,
      rpcUrl,
      fallbackRpcUrls: [],
      rpcUser: undefined,
      rpcPassword: undefined,
      requirePrimarySync: false,
    }, nodeOptions));
  }

  public async jsonRpc<T>(method: string, params: Record<string, unknown> = {}): Promise<T> {
    return this.withFallback(async (rpc) => {
      const result = await rpc.jsonRpc<T>(method, params);
      if (method === 'get_info' && rpc === this.primary) {
        // Every get_info that flows through the primary (the event bus
        // polls one every few seconds) doubles as a free health probe.
        this.recordPrimaryInfo(result as unknown as IMoneroApi.Info);
      }
      return result;
    }, `json-rpc ${method}`);
  }

  public async raw<T>(path: string, body: Record<string, unknown> = {}): Promise<T> {
    const normalized = path.startsWith('/') ? path : `/${path}`;
    if (this.skipsPrimary(normalized)) {
      return this.routeSkipPath((rpc) => rpc.raw<T>(normalized, body), `raw ${normalized}`, normalized);
    }
    return this.withFallback((rpc) => rpc.raw<T>(normalized, body), `raw ${normalized}`);
  }

  public async rawBytes(path: string, body: Buffer | Uint8Array): Promise<{ data: Buffer; contentType: string }> {
    const normalized = path.startsWith('/') ? path : `/${path}`;
    if (this.skipsPrimary(normalized)) {
      return this.routeSkipPath((rpc) => rpc.rawBytes(normalized, body), `raw-bytes ${normalized}`, normalized);
    }
    return this.withFallback((rpc) => rpc.rawBytes(normalized, body), `raw-bytes ${normalized}`);
  }

  /** Last known primary health, for diagnostics. */
  public primaryHealth(): { usable: boolean | null; checkedAt: number } {
    return { usable: this.primaryUsable, checkedAt: this.primaryCheckedAt };
  }

  private skipsPrimary(path: string): boolean {
    return MoneroRpcPool.PRIMARY_SKIP_PATHS.has(path);
  }

  private get healthIntervalMs(): number {
    return Math.max(1_000, this.config.primaryHealthCheckIntervalMs ?? 15_000);
  }

  /**
   * Sticky per-path routing for endpoints the primary may not serve.
   * Candidate order: last node that answered, then fallbacks in config
   * order, then the primary. Nodes that failed within the interval are
   * skipped; if every candidate is marked bad they are all tried anyway
   * (better a slow answer than none).
   */
  private async routeSkipPath<T>(call: (rpc: MoneroRpc) => Promise<T>, label: string, path: string): Promise<T> {
    const candidates = [...this.fallbacks, this.primary];
    const preferred = this.pathPreferred.get(path);
    const ordered = preferred ? [preferred, ...candidates.filter((c) => c !== preferred)] : candidates;
    const now = Date.now();
    const live = ordered.filter((rpc) => (this.pathBadUntil.get(`${rpc.rpcUrl}|${path}`) ?? 0) <= now);
    const attempts = live.length > 0 ? live : ordered;
    let lastErr: unknown;
    for (const rpc of attempts) {
      try {
        const result = await call(rpc);
        this.pathPreferred.set(path, rpc);
        this.pathBadUntil.delete(`${rpc.rpcUrl}|${path}`);
        return result;
      } catch (err) {
        lastErr = err;
        this.pathBadUntil.set(`${rpc.rpcUrl}|${path}`, Date.now() + this.healthIntervalMs);
        if (this.pathPreferred.get(path) === rpc) {
          this.pathPreferred.delete(path);
        }
        this.warn(`${rpc.rpcUrl} failed ${label}; skipping it for ${this.healthIntervalMs}ms: ${formatError(err)}`);
      }
    }
    throw lastErr;
  }

  private async withFallback<T>(call: (rpc: MoneroRpc) => Promise<T>, label: string): Promise<T> {
    const selected = await this.selectRpc();
    const others = selected === this.primary
      ? this.liveFallbacks()
      : [...this.liveFallbacks().filter((rpc) => rpc !== selected), this.primary];
    let lastErr: unknown;
    try {
      return await call(selected);
    } catch (err) {
      lastErr = err;
      this.markFailed(selected);
      if (others.length === 0) {
        throw err;
      }
      this.warn(`${selected.rpcUrl} failed ${label}; failing over: ${formatError(err)}`);
    }
    for (const rpc of others) {
      try {
        const result = await call(rpc);
        this.fallbackBadUntil.delete(rpc.rpcUrl);
        return result;
      } catch (err) {
        lastErr = err;
        this.markFailed(rpc);
        this.warn(`${rpc.rpcUrl} failed ${label}: ${formatError(err)}`);
      }
    }
    throw lastErr;
  }

  /** Fallbacks not marked bad within the interval; all of them if every one is (better slow than nothing). */
  private liveFallbacks(): MoneroRpc[] {
    const now = Date.now();
    const live = this.fallbacks.filter((rpc) => (this.fallbackBadUntil.get(rpc.rpcUrl) ?? 0) <= now);
    return live.length > 0 ? live : this.fallbacks;
  }

  private markFailed(rpc: MoneroRpc): void {
    if (rpc === this.primary) {
      this.markPrimaryUnusable();
    } else {
      this.fallbackBadUntil.set(rpc.rpcUrl, Date.now() + this.healthIntervalMs);
    }
  }

  private async selectRpc(): Promise<MoneroRpc> {
    if (this.fallbacks.length === 0) {
      return this.primary;
    }
    return await this.isPrimaryUsable() ? this.primary : this.liveFallbacks()[0];
  }

  /**
   * Stale-while-revalidate health state. Only the very first call (no
   * state yet) waits for a probe; afterwards callers get the last known
   * answer immediately and a single background probe refreshes it once
   * it is older than the interval.
   */
  private async isPrimaryUsable(): Promise<boolean> {
    const now = Date.now();
    if (this.primaryUsable === null) {
      return this.probePrimary();
    }
    if (now - this.primaryCheckedAt >= this.healthIntervalMs && !this.probeInflight) {
      void this.probePrimary();
    }
    return this.primaryUsable;
  }

  private probePrimary(): Promise<boolean> {
    if (this.probeInflight) {
      return this.probeInflight;
    }
    this.probeInflight = (async () => {
      try {
        const info = await this.primaryProbe.jsonRpc<IMoneroApi.Info>('get_info');
        return this.recordPrimaryInfo(info);
      } catch (err) {
        this.primaryUsable = false;
        this.primaryCheckedAt = Date.now();
        this.warn(`primary ${this.primary.rpcUrl} health check failed; using fallback ${this.fallbacks[0]?.rpcUrl}: ${formatError(err)}`);
        return false;
      } finally {
        this.probeInflight = null;
      }
    })();
    return this.probeInflight;
  }

  private recordPrimaryInfo(info: IMoneroApi.Info): boolean {
    const status = this.config.requirePrimarySync
      ? daemonSyncStatus(info, this.config.maxPrimaryHeightLag ?? 10)
      : { usable: true, reason: 'ready' };
    this.primaryUsable = status.usable;
    this.primaryCheckedAt = Date.now();
    if (!status.usable) {
      this.warn(`primary ${this.primary.rpcUrl} not ready (${status.reason}); using fallback ${this.fallbacks[0]?.rpcUrl}`);
    }
    return status.usable;
  }

  private markPrimaryUnusable(): void {
    this.primaryUsable = false;
    this.primaryCheckedAt = Date.now();
  }

  private warn(message: string): void {
    const now = Date.now();
    if (message === this.lastWarning && now - this.lastWarningAt < 60_000) {
      return;
    }
    this.lastWarning = message;
    this.lastWarningAt = now;
    // eslint-disable-next-line no-console
    console.warn(`[xmr-space] monerod fallback: ${message}`);
  }
}

function daemonSyncStatus(info: IMoneroApi.Info, maxLag: number): { usable: boolean; reason: string } {
  if (info.status && info.status !== 'OK') {
    return { usable: false, reason: `status=${info.status}` };
  }
  if (info.busy_syncing === true) {
    return { usable: false, reason: 'busy_syncing=true' };
  }
  if (info.synchronized === false) {
    return { usable: false, reason: 'synchronized=false' };
  }
  const height = Number(info.height ?? 0);
  const targetHeight = Number(info.target_height ?? 0);
  if (targetHeight > 0 && height + maxLag < targetHeight) {
    return { usable: false, reason: `height=${height}, target_height=${targetHeight}` };
  }
  return { usable: true, reason: 'ready' };
}

function isTimeoutError(err: unknown): boolean {
  if (!axios.isAxiosError(err)) {
    return false;
  }
  if (err.code && ['ETIMEDOUT', 'ECONNABORTED'].includes(err.code)) {
    return true;
  }
  return /timeout/i.test(err.message);
}

function isTransientRpcError(err: unknown): boolean {
  if (!axios.isAxiosError(err)) {
    return false;
  }
  const status = err.response?.status;
  if (status === 429 || (status !== undefined && status >= 500)) {
    return true;
  }
  const code = err.code;
  if (code && ['ECONNRESET', 'ETIMEDOUT', 'ECONNABORTED', 'EAI_AGAIN', 'ENOTFOUND'].includes(code)) {
    return true;
  }
  return /socket hang up|timeout|network error/i.test(err.message);
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function formatError(err: unknown): string {
  if (axios.isAxiosError(err)) {
    const status = err.response?.status ? ` HTTP ${err.response.status}` : '';
    return `${err.code ?? 'axios'}${status} ${err.message}`.trim();
  }
  return err instanceof Error ? err.message : String(err);
}
