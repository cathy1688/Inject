import type { Env } from './types';
import { handleApi } from './api';
import { cleanupOldData } from './db';
import { ensureSchema } from './schema';
import { ChainCollector, ensureCollector } from './collector';

export { ChainCollector };

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);
    if (url.pathname.startsWith('/api/')) {
      await ensureSchema(env);
      // Status polling is enough to prime the collector immediately after a
      // deployment; the minute cron remains the independent watchdog.
      if (url.pathname === '/api/status') {
        ctx.waitUntil(ensureCollector(env).catch(()=>undefined));
      }
      return handleApi(request, env);
    }
    return env.ASSETS.fetch(request);
  },

  async scheduled(_controller: ScheduledController, env: Env, ctx: ExecutionContext): Promise<void> {
    await ensureSchema(env);
    // Cron only guards the 12-second Durable Object loop. The actual chain scan
    // no longer depends on a browser tab or on this once-per-minute trigger.
    ctx.waitUntil(ensureCollector(env).catch(()=>undefined));

    const lastCleanup=Number((await env.META_DB.prepare("SELECT value FROM sync_state WHERE key='last_cleanup_ms'").first<{value:string}>())?.value??0);
    if(Date.now()-lastCleanup>20*60*60*1000){
      const retention=Math.min(30,Math.max(1,Number(env.RETENTION_DAYS??'30')));
      ctx.waitUntil(cleanupOldData(env,retention));
    }
  }
};
