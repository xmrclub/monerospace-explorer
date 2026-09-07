import { EventEmitter } from 'events';
import { IMoneroApi, MoneroDaemonConfig } from './monero-api.interface';
import { MoneroRpcPool } from './monero-rpc';
import { MoneroApi } from './monero-api';

/**
 * Polls monerod and emits high-level events. Reads go through `MoneroApi`
 * with `force=true`: the bus always sees a fresh view for change
 * detection, and the fetch it pays for lands in the shared cache so the
 * request paths (fees, mempool, dashboard snapshots) read it for free
 * instead of each fetching the pool synchronously.
 *
 * Why polling and not ZMQ: monerod's ZMQ pub/sub is great when you control
 * the daemon, but most public RPC endpoints (cakewallet, xmr.node.live,
 * supportxmr, …) don't expose it. Polling at 3s is the lowest-friction
 * path that works against any daemon and still meets the "push within 5s"
 * SLO since polling at 3s + a tiny fan-out cost stays under that ceiling.
 *
 * Events:
 *   - 'block'           → IMoneroApi.BlockHeader on every new tip
 *   - 'mempool-delta'   → { added: hashes[], removed: hashes[] }
 *   - 'snapshot'        → { info, mempoolHashes } pushed once on connect
 *
 * The bus is process-local — no Redis, no cluster fan-out. If we scale
 * out we'll need a fan-out layer; until then a single backend process
 * handles all SSE connections.
 */
export class MoneroEventBus extends EventEmitter {
  private rpc: MoneroRpcPool;
  private api: MoneroApi;
  private pollMs: number;
  private timer: NodeJS.Timeout | null = null;
  private lastHeight: number | null = null;
  private lastTipHash: string | null = null;
  private lastMempoolHashes = new Set<string>();
  private latestInfo: IMoneroApi.Info | null = null;
  private inflight = false;

  /**
   * Pass the shared `MoneroApi` so the bus and the request paths use one
   * transport (one health state, one set of caches). A bare config is
   * still accepted for tests and standalone use.
   */
  constructor(source: MoneroDaemonConfig | MoneroApi, pollMs = 3000) {
    super();
    // Bump max listeners — every SSE connection adds 2 (block + mempool).
    // Default of 10 trips alarms when more than ~5 dashboards are open.
    this.setMaxListeners(0);
    this.api = source instanceof MoneroApi ? source : new MoneroApi(source);
    this.rpc = this.api.pool;
    this.pollMs = pollMs;
  }

  /** Latest snapshot a new SSE client should receive on connect. */
  public snapshot(): { info: IMoneroApi.Info | null; mempoolHashes: string[] } {
    return { info: this.latestInfo, mempoolHashes: Array.from(this.lastMempoolHashes) };
  }

  public start(): void {
    if (this.timer) {
      return;
    }
    // Kick off a poll immediately so the first SSE client doesn't wait.
    void this.poll();
    this.timer = setInterval(() => void this.poll(), this.pollMs);
  }

  public stop(): void {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  /**
   * Single polling pass. Re-entrancy-guarded — if a previous poll is still
   * in flight (slow daemon, network hiccup) we skip rather than stack up
   * concurrent requests. Failures emit 'error' but don't kill the timer;
   * we want the bus to recover automatically when the daemon comes back.
   */
  private async poll(): Promise<void> {
    if (this.inflight) {
      return;
    }
    this.inflight = true;
    try {
      const info = await this.api.getInfo(true);
      this.latestInfo = info;
      const tipHeight = info.height - 1;
      if (this.lastHeight === null || this.lastTipHash === null) {
        // First poll — record state but don't fire a 'block' event. The
        // initial snapshot is delivered explicitly when SSE clients
        // connect, not as a wakeup event.
        this.lastHeight = tipHeight;
        this.lastTipHash = info.top_block_hash;
      } else if (info.top_block_hash !== this.lastTipHash) {
        // Tip moved — fetch the header and emit. We use top_block_hash
        // rather than height because reorgs can land at the same height
        // with a different hash (rare on Monero but possible).
        try {
          const block = await this.rpc.jsonRpc<{ block_header: IMoneroApi.BlockHeader }>(
            'get_block_header_by_hash',
            { hash: info.top_block_hash },
          );
          this.lastHeight = tipHeight;
          this.lastTipHash = info.top_block_hash;
          this.emit('block', block.block_header);
        } catch (err) {
          // Header fetch failed — log via 'error' but don't fail the poll.
          this.emit('error', err);
        }
      }

      // Mempool delta. The daemon's response is unsorted, so we work in
      // sets. A typical pool churn cycle is 5-15 txs/poll on mainnet.
      const pool = await this.api.getTransactionPool(true);
      const currentHashes = new Set((pool.transactions ?? []).map((t) => t.id_hash));
      const added: string[] = [];
      const removed: string[] = [];
      for (const h of currentHashes) {
        if (!this.lastMempoolHashes.has(h)) {
          added.push(h);
        }
      }
      for (const h of this.lastMempoolHashes) {
        if (!currentHashes.has(h)) {
          removed.push(h);
        }
      }
      this.lastMempoolHashes = currentHashes;
      if (added.length || removed.length) {
        this.emit('mempool-delta', { added, removed });
      }
    } catch (err) {
      this.emit('error', err);
    } finally {
      this.inflight = false;
    }
  }
}
