import { DurableObject } from 'cloudflare:workers';
import type { Env } from './types';
import { scanToFinalized } from './scanner';

const COLLECT_INTERVAL_MS = 12_000;
const COLLECT_MAX_BLOCKS = 8;
const COLLECTOR_NAME = 'main';

/**
 * Backend 12-second collector loop.
 *
 * Durable Object alarms keep this loop alive independently of any browser tab.
 * The minute cron in index.ts only acts as a watchdog that re-arms the loop if
 * Cloudflare ever evicts/restarts the object and no alarm remains scheduled.
 */
export class ChainCollector extends DurableObject<Env> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
  }

  async fetch(_request: Request): Promise<Response> {
    const current = await this.ctx.storage.getAlarm();
    if (current == null) {
      await this.ctx.storage.setAlarm(Date.now() + 1_000);
    }
    return new Response(JSON.stringify({ ok: true, alarmAt: current }), {
      headers: { 'content-type': 'application/json; charset=utf-8' }
    });
  }

  async alarm(): Promise<void> {
    const startedAt = Date.now();
    try {
      await scanToFinalized(this.env, COLLECT_MAX_BLOCKS);
    } catch (error) {
      console.warn('12s collector scan failed', error);
      // scanToFinalized records the RPC error in META_DB. Keep the recurring
      // alarm alive instead of exhausting Durable Object alarm retries.
    } finally {
      const nextAt = Math.max(Date.now() + 1_000, startedAt + COLLECT_INTERVAL_MS);
      await this.ctx.storage.setAlarm(nextAt);
    }
  }
}

export async function ensureCollector(env: Env): Promise<void> {
  const id = env.CHAIN_COLLECTOR.idFromName(COLLECTOR_NAME);
  const stub = env.CHAIN_COLLECTOR.get(id);
  const response = await stub.fetch('https://collector.internal/ensure');
  if (!response.ok) throw new Error(`Collector watchdog failed: HTTP ${response.status}`);
}
