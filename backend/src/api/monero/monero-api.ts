import memoryCache from '../memory-cache';
import { IMoneroApi, MoneroDaemonConfig } from './monero-api.interface';
import { MoneroRpcPool } from './monero-rpc';
import { createHash } from 'crypto';

/**
 * High-level monerod accessor with per-call server-side caching. The cache
 * windows are deliberately short (5–10s) — Monero blocks target 2 minutes,
 * but mempool / fee data churns fast enough that we want fresh reads while
 * still flattening burst traffic from many websocket clients hitting the
 * same endpoint at once.
 *
 * Cache windows by call:
 *   getInfo            — 5s   (used by the dashboard top bar; the event
 *                              bus refreshes it every poll so reads hit)
 *   getBlockCount      — 5s
 *   getBlockByHash     — 60s near the tip, 600s once 10+ blocks deep
 *   getBlockByHeight   — same
 *   getTransactionPool — 15s  (ceiling only: the event bus force-refreshes
 *                              it every poll, so reads are ~3s stale)
 *   getFeeEstimate     — 10s  (fee tiers move slowly)
 *
 * Identical calls that are already in flight share one promise, so a
 * burst of dashboards connecting in the same second costs the daemon one
 * fetch per distinct key, not one per client.
 *
 * NB: nothing here writes user-supplied keys anywhere. The daemon doesn't
 * have wallet endpoints exposed and we never proxy them.
 */
export class MoneroApi {
  private rpc: MoneroRpcPool;
  private inflight = new Map<string, Promise<unknown>>();
  /** Highest tip height seen from get_info / get_block_count; picks block cache TTLs. */
  private lastKnownTip = 0;

  /** Accepts a config (builds its own pool) or an existing pool to share with the event bus. */
  constructor(source: MoneroDaemonConfig | MoneroRpcPool) {
    this.rpc = source instanceof MoneroRpcPool ? source : new MoneroRpcPool(source);
  }

  /** The underlying transport, for callers that need the same health state (event bus). */
  public get pool(): MoneroRpcPool {
    return this.rpc;
  }

  /**
   * Cache read + single-flight fetch. Concurrent callers for the same key
   * await the same promise; the entry is removed on settle either way so
   * a rejected fetch never poisons the key — the next caller simply
   * fetches again.
   */
  private fetchOnce<T>(type: string, id: string, ttlSeconds: number | ((value: T) => number), fetcher: () => Promise<T>, force = false): Promise<T> {
    if (!force) {
      const cached = memoryCache.get<T>(type, id);
      if (cached !== null && cached !== undefined) {
        return Promise.resolve(cached);
      }
    }
    const key = `${type}:${id}`;
    const pending = this.inflight.get(key);
    if (pending) {
      return pending as Promise<T>;
    }
    const run = (async () => {
      const value = await fetcher();
      const ttl = typeof ttlSeconds === 'function' ? ttlSeconds(value) : ttlSeconds;
      memoryCache.set(type, id, value, ttl);
      return value;
    })();
    this.inflight.set(key, run);
    void run.finally(() => this.inflight.delete(key)).catch(() => undefined);
    return run;
  }

  private noteTip(height: number): void {
    if (Number.isFinite(height) && height > this.lastKnownTip) {
      this.lastKnownTip = height;
    }
  }

  /** Confirmed blocks 10+ deep are immutable in practice; near the tip keep the short window. */
  private blockTtl(block: IMoneroApi.Block): number {
    const height = Number(block.block_header?.height ?? 0);
    return this.lastKnownTip > 0 && this.lastKnownTip - height >= 10 ? 600 : 60;
  }

  /**
   * Daemon info: height, hashrate-derivable difficulty, mempool size, version.
   * `force` bypasses the cache read (the event bus uses it for change
   * detection) but still populates the cache for everyone else.
   */
  public async getInfo(force = false): Promise<IMoneroApi.Info> {
    const info = await this.fetchOnce<IMoneroApi.Info>('xmr', 'info', 5, () => this.rpc.jsonRpc<IMoneroApi.Info>('get_info'), force);
    this.noteTip(Number(info.height ?? 0) - 1);
    return info;
  }

  /**
   * Forward an already-whitelisted public JSON-RPC daemon method.
   * The route layer validates method names and rejects secret-shaped
   * request bodies before reaching this transport helper.
   */
  public async proxyPublicJsonRpc<T>(method: string, params: Record<string, unknown> = {}): Promise<T> {
    return this.rpc.jsonRpc<T>(method, params);
  }

  /** Forward an already-whitelisted public daemon path request. */
  public async proxyPublicRaw<T>(path: string, body: Record<string, unknown> = {}): Promise<T> {
    return this.rpc.raw<T>(path, body);
  }

  /** Forward an already-whitelisted public binary daemon path request. */
  public async proxyPublicRawBytes(path: string, body: Buffer | Uint8Array): Promise<{ data: Buffer; contentType: string }> {
    return this.rpc.rawBytes(path, body);
  }

  /** Just the height — cheaper than `getInfo` when that's all the caller needs. */
  public async getBlockCount(): Promise<number> {
    const count = await this.fetchOnce<number>('xmr', 'blockcount', 5, async () => {
      const result = await this.rpc.jsonRpc<IMoneroApi.BlockCount>('get_block_count');
      return result.count;
    });
    this.noteTip(count - 1);
    return count;
  }

  /** Full block by hash (header + miner tx + tx hashes). */
  public async getBlockByHash(hash: string): Promise<IMoneroApi.Block> {
    return this.fetchOnce<IMoneroApi.Block>('xmr-block-hash', hash, (b) => this.blockTtl(b),
      () => this.rpc.jsonRpc<IMoneroApi.Block>('get_block', { hash }));
  }

  /** Full block by height. */
  public async getBlockByHeight(height: number): Promise<IMoneroApi.Block> {
    return this.fetchOnce<IMoneroApi.Block>('xmr-block-height', String(height), (b) => this.blockTtl(b),
      () => this.rpc.jsonRpc<IMoneroApi.Block>('get_block', { height }));
  }

  /**
   * Bulk header fetch via `get_block_headers_range`. Range is inclusive
   * on both ends and capped at 999 by monerod. Used by the chain
   * indexer to backfill difficulty for many heights without paying
   * the per-call RPC overhead of `getBlockByHeight`.
   */
  public async getBlockHeadersRange(startHeight: number, endHeight: number): Promise<IMoneroApi.BlockHeader[]> {
    if (endHeight < startHeight) return [];
    if (endHeight - startHeight > 999) {
      throw new Error(`get_block_headers_range max span is 999 (got ${endHeight - startHeight + 1})`);
    }
    const result = await this.rpc.jsonRpc<{ headers: IMoneroApi.BlockHeader[] }>(
      'get_block_headers_range',
      { start_height: startHeight, end_height: endHeight },
    );
    return result.headers ?? [];
  }

  /**
   * Mempool snapshot — list of pending txs with fees, weights, ages.
   * The event bus calls this with `force=true` every poll, which keeps the
   * cached copy a few seconds old for every request path (fees, mempool,
   * dashboard snapshots) without any of them fetching synchronously.
   *
   * Served by the primary (mnr.network verifies each entry: blob hashes to
   * its id, fee/weight/size recomputed) unless MONEROD_RPC_FALLBACK_FIRST_PATHS
   * routes it elsewhere. Downstream reads id_hash, fee, weight, blob_size,
   * receive_time, relayed, double_spend_seen and tx_json, all of which mnr
   * populates; the daemon-internal fields it leaves empty (last_failed_*,
   * max_used_block_*, kept_by_block, do_not_relay, last_relayed_time) are
   * not used anywhere.
   */
  public async getTransactionPool(force = false): Promise<IMoneroApi.TransactionPool> {
    return this.fetchOnce<IMoneroApi.TransactionPool>('xmr', 'mempool', 15,
      () => this.rpc.raw<IMoneroApi.TransactionPool>('/get_transaction_pool'), force);
  }

  /**
   * Look up confirmed transactions by hash via `/get_transactions`. Returns
   * the entries the daemon was able to find — callers should match by
   * `tx_hash` since the daemon will silently omit unknowns.
   *
   * `decode_as_json=true` instructs the daemon to populate `as_json` on
   * each entry: a string that JSON-parses to `{version, unlock_time, vin,
   * vout, extra, rct_signatures}`. We pass `prune=true` so we don't pull
   * the giant rangeproof / bulletproof blobs we have no use for.
   *
   * Cache window: 30s. Confirmed tx data is immutable — could be cached
   * forever — but the wrapper response carries `confirmations`, which IS
   * dynamic. 30s is a reasonable compromise.
   */
  public async getTransactionsByHashes(hashes: string[]): Promise<IMoneroApi.TransactionEntry[]> {
    if (hashes.length === 0) {
      return [];
    }
    // Cache by sorted hash list; for single-hash lookups (the common case
    // in tx-detail views) this still hits the same key on repeated reads.
    const cacheKey = hashes.slice().sort().join(',');
    return this.fetchOnce<IMoneroApi.TransactionEntry[]>('xmr-tx', cacheKey, 30, async () => {
      const resp = await this.rpc.raw<{ txs?: IMoneroApi.TransactionEntry[]; status: string }>(
        '/get_transactions',
        { txs_hashes: hashes, decode_as_json: true, prune: true },
      );
      return resp.txs ?? [];
    });
  }

  /** Convenience wrapper for the single-hash case. Returns `null` if not found. */
  public async getTransactionByHash(hash: string): Promise<IMoneroApi.TransactionEntry | null> {
    const txs = await this.getTransactionsByHashes([hash]);
    return txs.find((t) => t.tx_hash === hash) ?? null;
  }

  /**
   * Resolve public ring-member output metadata by global output index.
   * For modern RingCT inputs the amount is 0; legacy pre-RingCT inputs
   * carry their explicit amount in the transaction's key image input.
   */
  public async getOuts(
    outputs: IMoneroApi.GetOutsRequestOutput[],
    getTxid = true,
  ): Promise<IMoneroApi.GetOutsOutput[]> {
    if (!outputs.length) {
      return [];
    }

    const cacheKey = `${getTxid ? 'txid' : 'no-txid'}:${outputs.map((o) => `${o.amount}:${o.index}`).join(',')}`;
    return this.fetchOnce<IMoneroApi.GetOutsOutput[]>('xmr-outs', cacheKey, 3600, async () => {
      const resp = await this.rpc.raw<IMoneroApi.GetOutsResponse>(
        '/get_outs',
        { outputs, get_txid: getTxid },
      );
      if (resp.status && resp.status !== 'OK') {
        throw new Error(`monerod /get_outs returned ${resp.status}`);
      }
      return resp.outs ?? [];
    });
  }

  /**
   * Compute aggregated public fee statistics for a confirmed block:
   * totalFees, medianFee (atomic/byte), minFee, maxFee, and a 7-bucket
   * feeRange [p0, p20, p40, p50, p60, p80, p100] used by the dashboard's
   * block tile to render the fee-tier color span.
   *
   * Cache forever-within-process: confirmed-block fees are immutable. The
   * cost without cache is proportional to (#txs × 1 daemon round trip)
   * because /get_transactions accepts the full list in one call up to
   * its server-configured batch limit (usually ~256 hashes). For typical
   * Monero blocks (10-200 txs) one call is enough.
   */
  public async getBlockFeeStats(blockHash: string, txHashes: string[]): Promise<{
    totalFees: number;
    medianFee: number;
    minFee: number;
    maxFee: number;
    feeRange: number[];
    nTx: number;
  }> {
    if (txHashes.length === 0) {
      return { totalFees: 0, medianFee: 0, minFee: 0, maxFee: 0, feeRange: [0, 0, 0, 0, 0, 0, 0], nTx: 0 };
    }
    // 24h cache — block fees never change but we don't want unbounded
    // memory growth across many days of uptime; the block-by-hash cache
    // already shares this lifecycle.
    return this.fetchOnce('xmr-block-fees', blockHash, 86_400, async () => {
      // /get_transactions has a server-side batch limit; chunk to be safe.
      const CHUNK = 100;
      const chunks: string[][] = [];
      for (let i = 0; i < txHashes.length; i += CHUNK) {
        chunks.push(txHashes.slice(i, i + CHUNK));
      }
      const allTxs: IMoneroApi.TransactionEntry[] = [];
      for (const chunk of chunks) {
        const got = await this.getTransactionsByHashes(chunk);
        allTxs.push(...got);
      }
      // Per-tx fee/byte rate. Each tx's wire blob length (pruned_as_hex
      // when present, else as_hex) gives bytes; rct_signatures.txnFee
      // gives fee. Skip the few entries that fail to JSON-parse.
      const rates: number[] = [];
      let totalFees = 0;
      for (const t of allTxs) {
        let fee = 0;
        try {
          const parsed = t.as_json ? JSON.parse(t.as_json) as IMoneroApi.TransactionJson : null;
          fee = parsed?.rct_signatures?.txnFee ?? 0;
        } catch {
          fee = 0;
        }
        const blobBytes = t.pruned_as_hex
          ? Math.floor(t.pruned_as_hex.length / 2)
          : t.as_hex
            ? Math.floor(t.as_hex.length / 2)
            : 0;
        if (blobBytes > 0 && fee > 0) {
          rates.push(fee / blobBytes);
        }
        totalFees += fee;
      }
      rates.sort((a, b) => a - b);
      const median = rates.length ? rates[Math.floor(rates.length / 2)] : 0;
      const minFee = rates.length ? rates[0] : 0;
      const maxFee = rates.length ? rates[rates.length - 1] : 0;
      const feeRange = rates.length
        ? [0, 0.2, 0.4, 0.5, 0.6, 0.8, 1].map((p) => rates[Math.min(rates.length - 1, Math.floor(p * (rates.length - 1)))])
        : [0, 0, 0, 0, 0, 0, 0];
      return { totalFees, medianFee: median, minFee, maxFee, feeRange, nTx: allTxs.length };
    });
  }

  /**
   * Per-tx public data for every tx in a confirmed block — drives the
   * block-detail page's WebGL tile visualization. Returns a list of
   * stripped tuples in the upstream's `TransactionStripped` shape:
   *   { txid, fee, vsize, value, rate, flags, time, acc }
   * value is always 0 (RingCT-hidden), flags 0 (Bitcoin-only), acc 0,
   * time is the block timestamp.
   *
   * Cached 24h per block hash.
   */
  public async getBlockStrippedTxs(blockHash: string, txHashes: string[], blockTimestamp: number): Promise<{
    txid: string;
    fee: number;
    vsize: number;
    value: number;
    rate: number;
    flags: number;
    time: number;
    acc: boolean;
  }[]> {
    if (txHashes.length === 0) {
      return [];
    }
    const cacheKey = `${blockHash}:${hashTxSet(txHashes)}`;
    return this.fetchOnce('xmr-block-stripped', cacheKey, 86_400, async () => {
      const CHUNK = 100;
      const chunks: string[][] = [];
      for (let i = 0; i < txHashes.length; i += CHUNK) {
        chunks.push(txHashes.slice(i, i + CHUNK));
      }
      const all: IMoneroApi.TransactionEntry[] = [];
      for (const chunk of chunks) {
        const got = await this.getTransactionsByHashes(chunk);
        all.push(...got);
      }
      const stripped = all.map((t) => {
        let fee = 0;
        try {
          const parsed = t.as_json ? JSON.parse(t.as_json) as IMoneroApi.TransactionJson : null;
          fee = parsed?.rct_signatures?.txnFee ?? 0;
        } catch {
          fee = 0;
        }
        const vsize = t.pruned_as_hex
          ? Math.floor(t.pruned_as_hex.length / 2)
          : t.as_hex
            ? Math.floor(t.as_hex.length / 2)
            : 0;
        return {
          txid: t.tx_hash,
          fee,
          vsize,
          value: 0,
          rate: vsize > 0 ? fee / vsize : 0,
          flags: 0,
          time: blockTimestamp,
          acc: false,
        };
    });
    return stripped;
    });
  }

  /**
   * Monero's daemon fee estimate. Returns the base atomic-per-byte fee plus
   * a `fees` array `[slow, normal, fast, fastest]`. The websocket layer uses
   * this as a floor, then derives the displayed tiers from the live mempool.
   *
   * `grace_blocks=10` mirrors the wallet default and produces a slightly
   * more conservative slow tier.
   */
  public async getFeeEstimate(): Promise<IMoneroApi.FeeEstimate> {
    return this.fetchOnce<IMoneroApi.FeeEstimate>('xmr', 'fees', 10,
      () => this.rpc.jsonRpc<IMoneroApi.FeeEstimate>('get_fee_estimate', { grace_blocks: 10 }));
  }
}

function hashTxSet(txHashes: string[]): string {
  return createHash('sha256')
    .update(txHashes.join(','))
    .digest('hex');
}

/**
 * Build a configured singleton from environment. Kept as a separate
 * factory (rather than `export default new MoneroApi(...)`) so tests can
 * construct their own instance against a mock URL.
 */
export function moneroApiFromEnv(env: NodeJS.ProcessEnv = process.env): MoneroApi {
  return new MoneroApi(moneroDaemonConfigFromEnv(env));
}

export function moneroDaemonConfigFromEnv(env: NodeJS.ProcessEnv = process.env): MoneroDaemonConfig {
  const rpcUrl = env.MONEROD_RPC_URL ?? 'https://xmr-node.cakewallet.com:18081';
  const timeoutMs = Number(env.MONEROD_RPC_TIMEOUT_MS ?? 10_000);
  const primaryTimeoutRaw = Number(env.MONEROD_RPC_PRIMARY_TIMEOUT_MS ?? NaN);
  const fallbackRpcUrls = parseRpcUrls(env.MONEROD_RPC_FALLBACK_URLS ?? env.MONEROD_RPC_FALLBACK_URL);
  return {
    rpcUrl,
    fallbackRpcUrls,
    rpcUser: env.MONEROD_RPC_USER,
    rpcPassword: env.MONEROD_RPC_PASSWORD,
    timeoutMs,
    primaryTimeoutMs: Number.isFinite(primaryTimeoutRaw) && primaryTimeoutRaw > 0 ? primaryTimeoutRaw : undefined,
    requirePrimarySync: parseBool(env.MONEROD_RPC_REQUIRE_SYNC, fallbackRpcUrls.length > 0),
    maxPrimaryHeightLag: Number(env.MONEROD_RPC_MAX_HEIGHT_LAG ?? 10),
    primaryHealthCheckIntervalMs: Number(env.MONEROD_RPC_HEALTH_INTERVAL_MS ?? 15_000),
    fallbackFirstPaths: parseRpcUrls(env.MONEROD_RPC_FALLBACK_FIRST_PATHS),
  };
}

function parseRpcUrls(raw: string | undefined): string[] {
  return (raw ?? '')
    .split(',')
    .map((url) => url.trim())
    .filter(Boolean);
}

function parseBool(raw: string | undefined, fallback: boolean): boolean {
  if (raw === undefined || raw.trim() === '') {
    return fallback;
  }
  return ['1', 'true', 'yes', 'on'].includes(raw.trim().toLowerCase());
}
