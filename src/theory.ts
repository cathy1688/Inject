/**
 * Theory engine boundary.
 *
 * We intentionally do NOT approximate the theoretical injection value. The
 * dashboard is a verification tool, so a guessed formula would manufacture a
 * false deviation. Once the exact approved formula is locked, implement it here
 * using per-block chain state and return an exact RAO bigint.
 */
export interface TheoryInput {
  blockNumber: number;
  netuid: number;
  actualRao: bigint;
  chainBuyRao: bigint;
}

export function calculateTheoreticalInjectedRao(_input: TheoryInput): bigint | null {
  return null;
}
