export interface Env {
  META_DB: D1Database;
  BLOCKS_0: D1Database;
  BLOCKS_1: D1Database;
  BLOCKS_2: D1Database;
  BLOCKS_3: D1Database;
  ASSETS: Fetcher;
  SUBTENSOR_WS_URL?: string;
  RETENTION_DAYS?: string;
}

/** Compact block value: [actual injected RAO, Chain Buys RAO, theoretical injected RAO]. */
export type SubnetBlockValue = [string, string, string | null];
export type BlockPayload = Record<string, SubnetBlockValue>;

/** Compact summary value: [actual RAO, Chain Buys RAO, theoretical RAO, block count]. */
export type SubnetSummaryValue = [string, string, string | null, number];
export type SummaryPayload = Record<string, SubnetSummaryValue>;

export interface SubnetRecord {
  netuid: number;
  registration_counter: string | null;
  name: string;
  status: 'active' | 'deregistered';
  first_seen_block: number;
  last_seen_block: number;
  updated_at_ms: number;
}
