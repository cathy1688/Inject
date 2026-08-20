/**
 * Runtime-aligned theoretical pool injection.
 *
 * Subtensor get_subnet_terms() does:
 *   alpha_in = tao_emission / price
 *   alpha_cap = root_proportion * alpha_emission
 *   if alpha_in > alpha_cap:
 *     alpha_in = alpha_cap
 *     tao_in = alpha_in * price
 *
 * Therefore:
 *   theoretical_tao_in = min(tao_emission, root_proportion * alpha_emission * price)
 *
 * Units here are integer RAO / alpha-RAO. Root proportion is the raw U96F32
 * representation (32 fractional bits). Runtime price API is scaled by 1e9.
 */
const FIXED_32 = 1n << 32n;
const PRICE_SCALE = 1_000_000_000n;

export interface TheoryInput {
  blockNumber: number;
  netuid: number;
  actualRao: bigint;
  chainBuyRao: bigint;
  rootProportionRaw: bigint;
  alphaEmissionRao: bigint;
  priceRaoPerAlpha: bigint;
}

export function calculateTheoreticalInjectedRao(input: TheoryInput): bigint {
  const taoEmissionRao = input.actualRao + input.chainBuyRao;
  if (taoEmissionRao <= 0n) return 0n;
  if (input.rootProportionRaw <= 0n || input.alphaEmissionRao <= 0n || input.priceRaoPerAlpha <= 0n) return 0n;

  // Match the runtime's fixed-point multiplication and eventual u64 truncation.
  const alphaInjectionCapRao = (input.rootProportionRaw * input.alphaEmissionRao) / FIXED_32;
  const taoInjectionCapRao = (alphaInjectionCapRao * input.priceRaoPerAlpha) / PRICE_SCALE;

  return taoEmissionRao < taoInjectionCapRao ? taoEmissionRao : taoInjectionCapRao;
}
