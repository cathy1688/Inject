interface JsonRpcResponse<T> {
  jsonrpc: '2.0';
  id: number;
  result?: T;
  error?: { code: number; message: string; data?: unknown };
}

interface StorageChangeSet {
  block: string;
  changes: Array<[string, string | null]>;
}

export interface RpcSubnetPrice {
  netuid: number;
  price: number | string;
}

const DEFAULT_ARCHIVE_ENDPOINTS = [
  'wss://archive.chain.opentensor.ai:443',
  'wss://archive.sub.latent.to:443'
] as const;

function errorText(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function isPrunedHistoricalStateError(error: unknown): boolean {
  const text = errorText(error).toLowerCase();
  return text.includes('unknownblock') || text.includes('state already discarded') || text.includes('state discarded');
}

export class SubtensorRpc {
  private ws: WebSocket | null = null;
  private id = 0;
  private pending = new Map<number, { resolve: (value: unknown) => void; reject: (reason?: unknown) => void; timer: ReturnType<typeof setTimeout> }>();
  private readonly historicalFallbackUrls: readonly string[];

  constructor(private readonly url: string, historicalFallbackUrls?: readonly string[]) {
    // Normal Finney nodes prune old state. Keep them as the fast primary for live
    // reads, but transparently retry pruned historical state against archive RPC.
    this.historicalFallbackUrls = historicalFallbackUrls ??
      (url.includes('entrypoint-finney') ? DEFAULT_ARCHIVE_ENDPOINTS : []);
  }

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
        // Ignore non-JSON notifications. This client does not subscribe.
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

  /** Retry one pruned historical operation on archive nodes, without changing the live primary connection. */
  private async withHistoricalFallback<T>(
    originalError: unknown,
    operation: (rpc: SubtensorRpc) => Promise<T>
  ): Promise<T> {
    if (!isPrunedHistoricalStateError(originalError) || !this.historicalFallbackUrls.length) throw originalError;

    const errors: string[] = [];
    for (const endpoint of this.historicalFallbackUrls) {
      const archive = new SubtensorRpc(endpoint, []);
      try {
        await archive.connect();
        return await operation(archive);
      } catch (error) {
        errors.push(`${endpoint}: ${errorText(error)}`);
      } finally {
        archive.close();
      }
    }

    throw new Error(`${errorText(originalError)}; archive fallback failed: ${errors.join(' | ')}`);
  }

  finalizedHead(): Promise<string> { return this.call<string>('chain_getFinalizedHead'); }
  async header(hash: string): Promise<{ number: string; parentHash: string }> { return this.call('chain_getHeader', [hash]); }
  blockHash(blockNumber: number): Promise<string | null> { return this.call<string | null>('chain_getBlockHash', [blockNumber]); }

  async currentAlphaPricesAll(atHash: string): Promise<RpcSubnetPrice[]> {
    try {
      return await this.call<RpcSubnetPrice[]>('swap_currentAlphaPriceAll', [atHash]);
    } catch (error) {
      return this.withHistoricalFallback(error, rpc => rpc.currentAlphaPricesAll(atHash));
    }
  }

  async currentAlphaPricesAllScale(atHash: string): Promise<string> {
    try {
      return await this.call<string>('state_call', ['SwapRuntimeApi_current_alpha_price_all', '0x', atHash]);
    } catch (error) {
      return this.withHistoricalFallback(error, rpc => rpc.currentAlphaPricesAllScale(atHash));
    }
  }

  /**
   * Query many storage keys in one normal JSON-RPC request. The public Finney
   * endpoint timed out on JSON-RPC batch arrays of many `state_getStorage`
   * requests. Substrate exposes `state_queryStorageAt` specifically for
   * querying a vector of storage keys at one block.
   *
   * If the live node has already pruned `atHash`, only that historical request
   * is retried against archive RPC. Current-state traffic remains on Finney.
   */
  async queryStorage(keys: string[], atHash: string): Promise<Map<string, string | null>> {
    try {
      const map = new Map<string, string | null>();
      const chunkSize = 256;
      for (let i = 0; i < keys.length; i += chunkSize) {
        const chunk = keys.slice(i, i + chunkSize);
        chunk.forEach(key => map.set(key, null));
        const sets = await this.call<StorageChangeSet[]>('state_queryStorageAt', [chunk, atHash]);
        for (const set of sets ?? []) {
          for (const [key, value] of set.changes ?? []) map.set(key, value);
        }
      }
      return map;
    } catch (error) {
      return this.withHistoricalFallback(error, rpc => rpc.queryStorage(keys, atHash));
    }
  }

  async keysPaged(prefix: string, atHash: string, pageSize = 512): Promise<string[]> {
    const all: string[] = [];
    let startKey: string | null = null;
    while (true) {
      const page = await this.call<string[]>('state_getKeysPaged', [prefix, pageSize, startKey, atHash]);
      if (!page?.length) break;
      all.push(...page);
      if (page.length < pageSize) break;
      const next = page[page.length - 1];
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
