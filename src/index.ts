import type { Env } from './types';
import { handleApi } from './api';
import { cleanupOldData } from './db';
import { scanToFinalized } from './scanner';

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);
    if (url.pathname.startsWith('/api/')) {
      // Query-time catch-up keeps the dashboard close to the latest finalized block.
      if (request.method === 'GET' && url.pathname !== '/api/status') {
        ctx.waitUntil(scanToFinalized(env, 8).catch(() => undefined));
      }
      return handleApi(request, env);
    }
    return env.ASSETS.fetch(request);
  },

  async scheduled(_controller: ScheduledController, env: Env, ctx: ExecutionContext): Promise<void> {
    ctx.waitUntil(scanToFinalized(env, 24).catch(() => undefined));
    const lastCleanup = Number((await env.META_DB.prepare("SELECT value FROM sync_state WHERE key='last_cleanup_ms'").first<{value:string}>())?.value ?? 0);
    if (Date.now() - lastCleanup > 20 * 60 * 60 * 1000) {
      const retention = Math.min(30, Math.max(1, Number(env.RETENTION_DAYS ?? '30')));
      ctx.waitUntil(cleanupOldData(env, retention));
    }
  }
};
