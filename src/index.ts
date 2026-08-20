import type { Env } from './types';
import { handleApi } from './api';
import { cleanupOldData } from './db';
import { runTheoryBackfill, scanToFinalized } from './scanner';
import { ensureSchema } from './schema';

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);
    if (url.pathname.startsWith('/api/')) {
      await ensureSchema(env);
      // The 12-second live sync response is kept fast. Status polling advances a
      // tiny historical theory batch in waitUntil(), after the response path.
      if (request.method === 'GET' && url.pathname === '/api/status') {
        ctx.waitUntil(runTheoryBackfill(env, 2).catch(() => undefined));
      }
      return handleApi(request, env);
    }
    return env.ASSETS.fetch(request);
  },

  async scheduled(_controller: ScheduledController, env: Env, ctx: ExecutionContext): Promise<void> {
    await ensureSchema(env);
    ctx.waitUntil((async () => {
      await scanToFinalized(env, 24).catch(() => undefined);
      await runTheoryBackfill(env, 8).catch(() => undefined);
    })());
    const lastCleanup = Number((await env.META_DB.prepare("SELECT value FROM sync_state WHERE key='last_cleanup_ms'").first<{value:string}>())?.value ?? 0);
    if (Date.now() - lastCleanup > 20 * 60 * 60 * 1000) {
      const retention = Math.min(30, Math.max(1, Number(env.RETENTION_DAYS ?? '30')));
      ctx.waitUntil(cleanupOldData(env, retention));
    }
  }
};
