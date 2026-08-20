interface JsonRpcResponse<T> {
  jsonrpc: '2.0';
  id: number;
  result?: T;
  error?: { code: number; message: string; data?: unknown };
}

export class SubtensorRpc {
  private ws: WebSocket | null = null;
  private id = 0;
  private pending = new Map<number, { resolve: (value: unknown) => void; reject: (reason?: unknown) => void; timer: ReturnType<typeof setTimeout> }>();

  constructor(private readonly url: string) {}

  async connect(): Promise<void> {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) return;
    const ws = new WebSocket(this.url);
    this.ws = ws;

    await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('Subtensor WebSocket connection timeout')), 12_000);
      const onOpen = () => {
        clearTimeout(timer);
        cleanup();
        resolve();
      };
      const onError = () => {
        clearTimeout(timer);
        cleanup();
        reject(new Error(`Unable to connect to Subtensor WebSocket: ${this.url}`));
      };
      const cleanup = () => {
        ws.removeEventListener('open', onOpen);
        ws.removeEventListener('error', onError);
      };
      ws.addEventListener('open', onOpen);
      ws.addEventListener('error', onError);
    });

    ws.addEventListener('message', (event: MessageEvent) => {
      try {
        const msg = JSON.parse(String(event.data)) as JsonRpcResponse<unknown>;
        if (typeof msg.id !== 'number') return;
        const waiter = this.pending.get(msg.id);
        if (!waiter) return;
        clearTimeout(waiter.timer);
        this.pending.delete(msg.id);
        if (msg.error) waiter.reject(new Error(`RPC ${msg.error.code}: ${msg.error.message}`));
        else waiter.resolve(msg.result);
      } catch {
        // Ignore non-JSON notifications. We do not subscribe in this client.
      }
    });

    const rejectAll = () => {
      for (const [requestId, waiter] of this.pending) {
        clearTimeout(waiter.timer);
        waiter.reject(new Error('Subtensor WebSocket closed'));
        this.pending.delete(requestId);
      }
    };
    ws.addEventListener('close', rejectAll);
    ws.addEventListener('error', rejectAll);
  }

  async call<T>(method: string, params: unknown[] = []): Promise<T> {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) await this.connect();
    const ws = this.ws;
    if (!ws) throw new Error('WebSocket not initialized');
    const id = ++this.id;
    const result = new Promise<T>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new Error(`RPC timeout: ${method}`));
      }, 15_000);
      this.pending.set(id, { resolve: resolve as (value: unknown) => void, reject, timer });
    });
    ws.send(JSON.stringify({ jsonrpc: '2.0', id, method, params }));
    return result;
  }

  finalizedHead(): Promise<string> {
    return this.call<string>('chain_getFinalizedHead');
  }

  async header(hash: string): Promise<{ number: string; parentHash: string }> {
    return this.call('chain_getHeader', [hash]);
  }

  blockHash(blockNumber: number): Promise<string | null> {
    return this.call<string | null>('chain_getBlockHash', [blockNumber]);
  }

  async queryStorage(keys: string[], atHash: string): Promise<Map<string, string | null>> {
    if (!keys.length) return new Map();
    const sets = await this.call<Array<{ block: string; changes: Array<[string, string | null]> }>>('state_queryStorageAt', [keys, atHash]);
    const map = new Map<string, string | null>();
    for (const set of sets ?? []) for (const [key, value] of set.changes ?? []) map.set(key, value);
    return map;
  }

  async keysPaged(prefix: string, atHash: string, pageSize = 512): Promise<string[]> {
    const all: string[] = [];
    let startKey: string | null = null;
    while (true) {
      const page: string[] = await this.call<string[]>('state_getKeysPaged', [prefix, pageSize, startKey, atHash]);
      if (!page?.length) break;
      all.push(...page);
      if (page.length < pageSize) break;
      const next: string = page[page.length - 1];
      if (next === startKey) break;
      startKey = next;
    }
    return all;
  }

  close(): void {
    try { this.ws?.close(1000, 'done'); } catch {}
    this.ws = null;
  }
}

export function parseBlockNumber(hexNumber: string): number {
  const n = Number.parseInt(hexNumber, 16);
  if (!Number.isSafeInteger(n)) throw new Error(`Unsafe block number: ${hexNumber}`);
  return n;
}
