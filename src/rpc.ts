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
      const onOpen = () => { clearTimeout(timer); cleanup(); resolve(); };
      const onError = () => { clearTimeout(timer); cleanup(); reject(new Error(`Unable to connect to Subtensor WebSocket: ${this.url}`)); };
      const cleanup = () => { ws.removeEventListener('open', onOpen); ws.removeEventListener('error', onError); };
      ws.addEventListener('open', onOpen);
      ws.addEventListener('error', onError);
    });

    ws.addEventListener('message', (event: MessageEvent) => {
      try {
        const parsed = JSON.parse(String(event.data)) as JsonRpcResponse<unknown> | JsonRpcResponse<unknown>[];
        const messages = Array.isArray(parsed) ? parsed : [parsed];
        for (const msg of messages) {
          if (typeof msg?.id !== 'number') continue;
          const waiter = this.pending.get(msg.id);
          if (!waiter) continue;
          clearTimeout(waiter.timer);
          this.pending.delete(msg.id);
          if (msg.error) waiter.reject(new Error(`RPC ${msg.error.code}: ${msg.error.message}`));
          else waiter.resolve(msg.result);
        }
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

  private prepare<T>(method: string): { id: number; promise: Promise<T> } {
    const id = ++this.id;
    const promise = new Promise<T>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new Error(`RPC timeout: ${method}`));
      }, 20_000);
      this.pending.set(id, { resolve: resolve as (value: unknown) => void, reject, timer });
    });
    return { id, promise };
  }

  async call<T>(method: string, params: unknown[] = []): Promise<T> {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) await this.connect();
    const ws = this.ws;
    if (!ws) throw new Error('WebSocket not initialized');
    const prepared = this.prepare<T>(method);
    ws.send(JSON.stringify({ jsonrpc: '2.0', id: prepared.id, method, params }));
    return prepared.promise;
  }

  async batch<T>(calls: Array<{ method: string; params?: unknown[] }>): Promise<T[]> {
    if (!calls.length) return [];
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) await this.connect();
    const ws = this.ws;
    if (!ws) throw new Error('WebSocket not initialized');
    const prepared = calls.map(call => ({ call, waiter: this.prepare<T>(call.method) }));
    ws.send(JSON.stringify(prepared.map(x => ({ jsonrpc: '2.0', id: x.waiter.id, method: x.call.method, params: x.call.params ?? [] }))));
    return Promise.all(prepared.map(x => x.waiter.promise));
  }

  finalizedHead(): Promise<string> { return this.call<string>('chain_getFinalizedHead'); }
  async header(hash: string): Promise<{ number: string; parentHash: string }> { return this.call('chain_getHeader', [hash]); }
  blockHash(blockNumber: number): Promise<string | null> { return this.call<string | null>('chain_getBlockHash', [blockNumber]); }

  /**
   * Read the full storage state for every requested key at one block.
   * Do not use state_queryStorageAt here: the monitor needs complete values,
   * not a change-set style response, otherwise inactive/unchanged keys can be
   * misclassified and per-block emission becomes incomplete.
   */
  async queryStorage(keys: string[], atHash: string): Promise<Map<string, string | null>> {
    const map = new Map<string, string | null>();
    const chunkSize = 96;
    for (let i = 0; i < keys.length; i += chunkSize) {
      const chunk = keys.slice(i, i + chunkSize);
      const values = await this.batch<string | null>(chunk.map(key => ({ method: 'state_getStorage', params: [key, atHash] })));
      chunk.forEach((key, index) => map.set(key, values[index] ?? null));
    }
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
