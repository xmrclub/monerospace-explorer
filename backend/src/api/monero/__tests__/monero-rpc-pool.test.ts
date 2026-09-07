import { AddressInfo, Socket } from 'net';
import { createServer, IncomingMessage, Server, ServerResponse } from 'http';
import { MoneroRpcPool } from '../monero-rpc';

type RpcBody = { method?: string; params?: Record<string, unknown> };
type RpcResult = { status?: number; body?: unknown; hang?: boolean; delayMs?: number };
type RpcHandler = (path: string, body: RpcBody) => RpcResult;

interface TestServer {
  url: string;
  calls: RpcBody[];
  paths: string[];
  close: () => Promise<void>;
}

/**
 * Minimal monerod stand-in. A handler may answer, delay, or hang
 * (`hang: true` never responds — the client's timeout has to fire).
 */
async function makeRpcServer(handler: RpcHandler): Promise<TestServer> {
  const calls: RpcBody[] = [];
  const paths: string[] = [];
  const sockets = new Set<Socket>();
  const server = createServer((req: IncomingMessage, res: ServerResponse) => {
    let raw = '';
    req.on('data', (chunk) => {
      raw += chunk.toString('utf8');
    });
    req.on('end', () => {
      const body = raw ? JSON.parse(raw) as RpcBody : {};
      calls.push(body);
      paths.push(req.url ?? '/');
      const result = handler(req.url ?? '/', body);
      if (result.hang) {
        return;
      }
      const respond = (): void => {
        res.statusCode = result.status ?? 200;
        res.setHeader('Content-Type', 'application/json');
        res.end(JSON.stringify(result.body ?? {}));
      };
      if (result.delayMs) {
        setTimeout(respond, result.delayMs);
      } else {
        respond();
      }
    });
  });
  server.on('connection', (socket) => {
    sockets.add(socket);
    socket.on('close', () => sockets.delete(socket));
  });

  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const address = server.address() as AddressInfo;
  return {
    url: `http://127.0.0.1:${address.port}`,
    calls,
    paths,
    close: () => {
      for (const socket of sockets) {
        socket.destroy();
      }
      return closeServer(server);
    },
  };
}

function closeServer(server: Server): Promise<void> {
  return new Promise((resolve, reject) => {
    server.close((err) => err ? reject(err) : resolve());
  });
}

const syncedInfo = {
  status: 'OK',
  height: 1_000,
  target_height: 1_000,
  synchronized: true,
  top_block_hash: '00',
};

const okCount = (count = 1_000): RpcResult => ({ body: { result: { count, status: 'OK' } } });

function wait(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

describe('MoneroRpcPool', () => {
  let warnSpy: jest.SpyInstance;

  beforeEach(() => {
    warnSpy = jest.spyOn(console, 'warn').mockImplementation(() => undefined);
  });

  afterEach(() => {
    warnSpy.mockRestore();
  });

  it('uses fallback while the primary daemon is still syncing', async () => {
    const primary = await makeRpcServer((_path, body) => {
      if (body.method === 'get_info') {
        return {
          body: {
            result: {
              status: 'OK',
              height: 100,
              target_height: 1_000,
              synchronized: false,
              top_block_hash: '00',
            },
          },
        };
      }
      return { status: 500, body: { error: 'primary should not serve data while syncing' } };
    });
    const fallback = await makeRpcServer((_path, body) => ({
      body: { result: { count: body.method === 'get_block_count' ? 1_000 : 0, status: 'OK' } },
    }));

    try {
      const pool = new MoneroRpcPool({
        rpcUrl: primary.url,
        fallbackRpcUrls: [fallback.url],
        timeoutMs: 500,
        requirePrimarySync: true,
        primaryHealthCheckIntervalMs: 1_000,
      });

      const count = await pool.jsonRpc<{ count: number }>('get_block_count');

      expect(count.count).toBe(1_000);
      expect(primary.calls.map((call) => call.method)).toEqual(['get_info']);
      expect(fallback.calls.map((call) => call.method)).toEqual(['get_block_count']);
    } finally {
      await primary.close();
      await fallback.close();
    }
  });

  it('falls back when the synced primary fails a read, retrying a 5xx once at most', async () => {
    const primary = await makeRpcServer((_path, body) => {
      if (body.method === 'get_info') {
        return { body: { result: syncedInfo } };
      }
      return { status: 502, body: { error: 'temporary primary failure' } };
    });
    const fallback = await makeRpcServer(() => okCount());

    try {
      const pool = new MoneroRpcPool({
        rpcUrl: primary.url,
        fallbackRpcUrls: [fallback.url],
        timeoutMs: 500,
        requirePrimarySync: true,
        primaryHealthCheckIntervalMs: 1_000,
      });

      const count = await pool.jsonRpc<{ count: number }>('get_block_count');

      expect(count.count).toBe(1_000);
      expect(primary.calls[0].method).toBe('get_info');
      // One retry, not the single-node default of two: a second node is
      // a better bet than a third attempt at the same one.
      expect(primary.calls.filter((call) => call.method === 'get_block_count')).toHaveLength(2);
      expect(fallback.calls.map((call) => call.method)).toEqual(['get_block_count']);
    } finally {
      await primary.close();
      await fallback.close();
    }
  });

  it('retries a 429 exactly once before failing over', async () => {
    const primary = await makeRpcServer((_path, body) => {
      if (body.method === 'get_info') {
        return { body: { result: syncedInfo } };
      }
      return { status: 429, body: { error: 'rate limited' } };
    });
    const fallback = await makeRpcServer(() => okCount(777));

    try {
      const pool = new MoneroRpcPool({
        rpcUrl: primary.url,
        fallbackRpcUrls: [fallback.url],
        timeoutMs: 500,
        requirePrimarySync: true,
        primaryHealthCheckIntervalMs: 1_000,
      });

      const count = await pool.jsonRpc<{ count: number }>('get_block_count');

      expect(count.count).toBe(777);
      expect(primary.calls.filter((call) => call.method === 'get_block_count')).toHaveLength(2);
      expect(fallback.calls).toHaveLength(1);
    } finally {
      await primary.close();
      await fallback.close();
    }
  });

  it('fails over after a single timeout on a hung primary and does not re-probe it within the interval', async () => {
    const primary = await makeRpcServer((_path, body) => {
      if (body.method === 'get_info') {
        return { body: { result: syncedInfo } };
      }
      return { hang: true };
    });
    const fallback = await makeRpcServer(() => okCount(4_242));

    try {
      const pool = new MoneroRpcPool({
        rpcUrl: primary.url,
        fallbackRpcUrls: [fallback.url],
        timeoutMs: 400,
        requirePrimarySync: true,
        primaryHealthCheckIntervalMs: 5_000,
      });

      const startedAt = Date.now();
      const first = await pool.jsonRpc<{ count: number }>('get_block_count');
      const elapsed = Date.now() - startedAt;

      expect(first.count).toBe(4_242);
      // One timeout (400ms) plus the fallback round trip — never 3x.
      expect(elapsed).toBeLessThan(1_000);
      expect(primary.calls.filter((call) => call.method === 'get_block_count')).toHaveLength(1);

      // The primary is now marked unusable for the interval: the next read
      // goes straight to the fallback with no probe on the request path.
      const primaryCallsBefore = primary.calls.length;
      const second = await pool.jsonRpc<{ count: number }>('get_block_count');
      expect(second.count).toBe(4_242);
      expect(primary.calls.length).toBe(primaryCallsBefore);
      expect(fallback.calls).toHaveLength(2);
    } finally {
      await primary.close();
      await fallback.close();
    }
  });

  it('serves reads from the last known health state and re-probes in the background', async () => {
    const primary = await makeRpcServer((_path, body) => {
      if (body.method === 'get_info') {
        // A slow probe must never be paid by a user request.
        return { body: { result: syncedInfo }, delayMs: 300 };
      }
      return okCount(1_000);
    });
    const fallback = await makeRpcServer(() => okCount(1));

    try {
      const pool = new MoneroRpcPool({
        rpcUrl: primary.url,
        fallbackRpcUrls: [fallback.url],
        timeoutMs: 1_000,
        requirePrimarySync: true,
        primaryHealthCheckIntervalMs: 1_000, // the pool clamps to >= 1s
      });

      // First call: no state yet, so the probe is awaited once.
      const first = await pool.jsonRpc<{ count: number }>('get_block_count');
      expect(first.count).toBe(1_000);
      expect(primary.calls.filter((call) => call.method === 'get_info')).toHaveLength(1);

      await wait(1_100); // state is now stale

      const startedAt = Date.now();
      const second = await pool.jsonRpc<{ count: number }>('get_block_count');
      const elapsed = Date.now() - startedAt;
      expect(second.count).toBe(1_000);
      expect(elapsed).toBeLessThan(200); // did not wait for the 300ms probe

      await wait(400); // let the background probe land
      expect(primary.calls.filter((call) => call.method === 'get_info')).toHaveLength(2);
      expect(fallback.calls).toHaveLength(0);
    } finally {
      await primary.close();
      await fallback.close();
    }
  });

  it('routes /get_transaction_pool past a hung fallback to the primary and remembers who answered', async () => {
    const primary = await makeRpcServer((path, body) => {
      if (body.method === 'get_info') {
        return { body: { result: syncedInfo } };
      }
      if (path === '/get_transaction_pool') {
        return { body: { status: 'OK', transactions: [{ id_hash: 'aa' }] } };
      }
      return okCount();
    });
    const fallback = await makeRpcServer((path) => {
      if (path === '/get_transaction_pool') {
        return { hang: true };
      }
      return okCount();
    });

    try {
      const pool = new MoneroRpcPool({
        rpcUrl: primary.url,
        fallbackRpcUrls: [fallback.url],
        timeoutMs: 400,
        requirePrimarySync: true,
        primaryHealthCheckIntervalMs: 5_000,
      });

      const startedAt = Date.now();
      const first = await pool.raw<{ transactions: { id_hash: string }[] }>('/get_transaction_pool');
      expect(Date.now() - startedAt).toBeLessThan(1_000);
      expect(first.transactions[0].id_hash).toBe('aa');
      expect(fallback.paths).toEqual(['/get_transaction_pool']);
      expect(primary.paths).toEqual(['/get_transaction_pool']);

      // Sticky: the primary answered, so it is tried first now and the
      // hung fallback is skipped for the interval.
      const second = await pool.raw<{ transactions: { id_hash: string }[] }>('/get_transaction_pool');
      expect(second.transactions[0].id_hash).toBe('aa');
      expect(fallback.paths).toHaveLength(1);
      expect(primary.paths).toHaveLength(2);

      // Concurrent reads while the preference flips all still resolve.
      const both = await Promise.all([
        pool.raw<{ transactions: unknown[] }>('/get_transaction_pool'),
        pool.raw<{ transactions: unknown[] }>('/get_transaction_pool'),
      ]);
      expect(both.every((r) => r.transactions.length === 1)).toBe(true);
      expect(fallback.paths).toHaveLength(1);
    } finally {
      await primary.close();
      await fallback.close();
    }
  });

  it('marks the primary bad for /get_transaction_pool when it refuses the call and sticks to the node that answered', async () => {
    const primary = await makeRpcServer((path, body) => {
      if (body.method === 'get_info') {
        return { body: { result: syncedInfo } };
      }
      if (path === '/get_transaction_pool') {
        return { status: 403, body: { error: 'restricted' } };
      }
      return okCount();
    });
    const brokenFallback = await makeRpcServer(() => ({ status: 500, body: { error: 'nope' } }));
    const goodFallback = await makeRpcServer((path) => {
      if (path === '/get_transaction_pool') {
        return { body: { status: 'OK', transactions: [] } };
      }
      return okCount();
    });

    try {
      const pool = new MoneroRpcPool({
        rpcUrl: primary.url,
        fallbackRpcUrls: [brokenFallback.url, goodFallback.url],
        timeoutMs: 400,
        requirePrimarySync: true,
        primaryHealthCheckIntervalMs: 5_000,
      });
      // Candidate order is [fallbacks..., primary]; put the primary in the
      // middle by making the good fallback the last candidate.
      const first = await pool.raw<{ status: string }>('/get_transaction_pool');
      expect(first.status).toBe('OK');
      expect(brokenFallback.paths.filter((p) => p === '/get_transaction_pool')).toHaveLength(2); // 500 retried once
      expect(goodFallback.paths).toEqual(['/get_transaction_pool']);

      // Second call: the good fallback answered last time, so it goes
      // first; the broken fallback and the primary are not asked again.
      const second = await pool.raw<{ status: string }>('/get_transaction_pool');
      expect(second.status).toBe('OK');
      expect(brokenFallback.paths.filter((p) => p === '/get_transaction_pool')).toHaveLength(2);
      expect(primary.paths.filter((p) => p === '/get_transaction_pool')).toHaveLength(0);

      // A refusal on this path does not affect json-rpc routing.
      const count = await pool.jsonRpc<{ count: number }>('get_block_count');
      expect(count.count).toBe(1_000);
      expect(primary.calls.map((c) => c.method)).toEqual(['get_info', 'get_block_count']);
    } finally {
      await primary.close();
      await brokenFallback.close();
      await goodFallback.close();
    }
  });

  it('marks the primary bad when it refuses /get_transaction_pool after the fallbacks failed', async () => {
    const primary = await makeRpcServer((path, body) => {
      if (body.method === 'get_info') {
        return { body: { result: syncedInfo } };
      }
      if (path === '/get_transaction_pool') {
        return { status: 403, body: { error: 'restricted' } };
      }
      return okCount();
    });
    const fallback = await makeRpcServer(() => ({ status: 404, body: { error: 'not here' } }));

    try {
      const pool = new MoneroRpcPool({
        rpcUrl: primary.url,
        fallbackRpcUrls: [fallback.url],
        timeoutMs: 400,
        requirePrimarySync: true,
        primaryHealthCheckIntervalMs: 5_000,
      });
      await expect(pool.raw('/get_transaction_pool')).rejects.toThrow();
      expect(fallback.paths).toEqual(['/get_transaction_pool']);
      expect(primary.paths).toEqual(['/get_transaction_pool']);
      // Both candidates are bad: the pool degrades to trying them all again
      // rather than failing without asking anyone.
      await expect(pool.raw('/get_transaction_pool')).rejects.toThrow();
      expect(fallback.paths).toHaveLength(2);
      expect(primary.paths).toHaveLength(2);
    } finally {
      await primary.close();
      await fallback.close();
    }
  });

  it('remembers a hung fallback on the general path once the primary is down', async () => {
    const primary = await makeRpcServer((_path, body) => {
      if (body.method === 'get_info') {
        return { body: { result: syncedInfo } };
      }
      return { hang: true };
    });
    const hungFallback = await makeRpcServer(() => ({ hang: true }));
    const goodFallback = await makeRpcServer(() => okCount(99));

    try {
      const pool = new MoneroRpcPool({
        rpcUrl: primary.url,
        fallbackRpcUrls: [hungFallback.url, goodFallback.url],
        timeoutMs: 300,
        requirePrimarySync: true,
        primaryHealthCheckIntervalMs: 5_000,
      });

      const startedAt = Date.now();
      const first = await pool.jsonRpc<{ count: number }>('get_block_count');
      expect(first.count).toBe(99);
      // Two single timeouts (primary, hung fallback), then the good one.
      expect(Date.now() - startedAt).toBeLessThan(1_200);
      expect(hungFallback.calls).toHaveLength(1);

      // Next read: primary and the hung fallback are both skipped.
      const t1 = Date.now();
      const second = await pool.jsonRpc<{ count: number }>('get_block_count');
      expect(second.count).toBe(99);
      expect(Date.now() - t1).toBeLessThan(250);
      expect(hungFallback.calls).toHaveLength(1);
      expect(primary.calls.filter((c) => c.method === 'get_block_count')).toHaveLength(1);
    } finally {
      await primary.close();
      await hungFallback.close();
      await goodFallback.close();
    }
  });
});
