import type { Env } from './types';
import { handleApi } from './api';
import { runArchiveTheoryBackfill } from './archive-backfill';
import { cleanupOldData, getState } from './db';
import { scanToFinalized } from './scanner';
import { ensureSchema } from './schema';

const THEORY_BACKFILL_MAX_LIVE_LAG = 64;

async function liveLag(env: Env): Promise<number> {
  const [last, chain] = await Promise.all([
    getState(env.META_DB, 'last_finalized_block'),
    getState(env.META_DB, 'chain_finalized_block')
  ]);
  return Math.max(0, Number(chain ?? 0) - Number(last ?? 0));
}

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);
    if (url.pathname.startsWith('/api/')) {
      await ensureSchema(env);

      // Do not let historical theory reconstruction compete with a large live
      // catch-up for the same Worker's subrequest budget. Once the observed-data
      // cursor is near the head, status polling resumes small archive backfills.
      if (request.method === 'GET' && url.pathname === '/api/status') {
        const lag = await liveLag(env);
        if (lag <= THEORY_BACKFILL_MAX_LIVE_LAG) {
          ctx.waitUntil(runArchiveTheoryBackfill(env, 2).catch(() => undefined));
        }
      }
      return handleApi(request, env);
    }
    return env.ASSETS.fetch(request);
  },

  async scheduled(_controller: ScheduledController, env: Env, ctx: ExecutionContext): Promise<void> {
    await ensureSchema(env);
    ctx.waitUntil((async () => {
      const result = await scanToFinalized(env, 24).catch(() => null);
      if (result && result.remainingLag <= THEORY_BACKFILL_MAX_LIVE_LAG) {
        await runArchiveTheoryBackfill(env, 2).catch(() => undefined);
      }
    })());

    const lastCleanup = Number((await env.META_DB.prepare("SELECT value FROM sync_state WHERE key='last_cleanup_ms'").first<{value:string}>())?.value ?? 0);
    if (Date.now() - lastCleanup > 20 * 60 * 60 * 1000) {
      const retention = Math.min(30, Math.max(1, Number(env.RETENTION_DAYS ?? '30')));
      ctx.waitUntil(cleanupOldData(env, retention));
    }
  }
};
