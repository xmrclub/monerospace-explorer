import { AddressInfo } from 'net';
import { createServer, IncomingMessage, Server, ServerResponse } from 'http';
// testSetup.ts stubs the shared cache to an empty object; give it a real
// in-memory implementation so cache hits and misses behave as in production.
jest.mock('../../memory-cache', () => {
  const store = new Map<string, unknown>();
  return {
    __esModule: true,
    default: {
      get: (type: string, id: string) => store.has(`${type}:${id}`) ? store.get(`${type}:${id}`) : null,
      set: (type: string, id: string, data: unknown) => { store.set(`${type}:${id}`, data); },
    },
  };
});

import { MoneroApi } from '../monero-api';

type RpcBody = { method?: string; params?: Record<string, unknown> };

async function makeDaemon(): Promise<{ url: string; calls: { path: string; body: RpcBody }[]; close: () => Promise<void> }> {
  const calls: { path: string; body: RpcBody }[] = [];
  const server = createServer((req: IncomingMessage, res: ServerResponse) => {
    let raw = '';
    req.on('data', (chunk) => { raw += chunk.toString('utf8'); });
    req.on('end', () => {
      const body = raw ? JSON.parse(raw) as RpcBody : {};
      const path = req.url ?? '/';
      calls.push({ path, body });
      const respond = (payload: unknown): void => {
        res.setHeader('Content-Type', 'application/json');
        res.end(JSON.stringify(payload));
      };
      // Small delay so concurrent callers overlap in flight.
      setTimeout(() => {
        if (path === '/get_transaction_pool') {
          respond({ status: 'OK', transactions: [{ id_hash: 'aa', fee: 1, weight: 1 }] });
          return;
        }
        if (body.method === 'get_block') {
          const height = Number(body.params?.height ?? 0);
          respond({ result: { block_header: { height, hash: `h${height}` }, tx_hashes: [] } });
          return;
        }
        if (body.method === 'get_block_count') {
          respond({ result: { count: 5_000, status: 'OK' } });
          return;
        }
        respond({ result: { status: 'OK' } });
      }, 50);
    });
  });
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const address = server.address() as AddressInfo;
  return {
    url: `http://127.0.0.1:${address.port}`,
    calls,
    close: () => new Promise<void>((resolve, reject) => server.close((err) => err ? reject(err) : resolve())),
  };
}

describe('MoneroApi single-flight', () => {
  let daemon: Awaited<ReturnType<typeof makeDaemon>>;
  let api: MoneroApi;

  beforeAll(async () => {
    daemon = await makeDaemon();
    api = new MoneroApi({ rpcUrl: daemon.url, timeoutMs: 1_000 });
  });

  afterAll(async () => {
    await daemon.close();
  });

  it('collapses concurrent identical block reads into one daemon call', async () => {
    const blocks = await Promise.all([1, 2, 3, 4, 5].map(() => api.getBlockByHeight(4_321)));
    expect(blocks.every((b) => b.block_header.hash === 'h4321')).toBe(true);
    expect(daemon.calls.filter((c) => c.body.method === 'get_block')).toHaveLength(1);
    // And a later read is served from cache.
    await api.getBlockByHeight(4_321);
    expect(daemon.calls.filter((c) => c.body.method === 'get_block')).toHaveLength(1);
  });

  it('lets a forced pool refresh warm the cache for plain readers', async () => {
    const before = daemon.calls.filter((c) => c.path === '/get_transaction_pool').length;
    const [forced, plain] = await Promise.all([api.getTransactionPool(true), api.getTransactionPool()]);
    expect(forced.transactions?.[0]?.id_hash).toBe('aa');
    expect(plain.transactions?.[0]?.id_hash).toBe('aa');
    await api.getTransactionPool();
    expect(daemon.calls.filter((c) => c.path === '/get_transaction_pool').length - before).toBe(1);
  });

  it('does not poison the key when a shared fetch rejects', async () => {
    const badApi = new MoneroApi({ rpcUrl: 'http://127.0.0.1:1', timeoutMs: 300 });
    await expect(badApi.getBlockByHeight(9)).rejects.toThrow();
    // Second attempt fetches again rather than returning the stale rejection.
    await expect(badApi.getBlockByHeight(9)).rejects.toThrow();
  });
});
