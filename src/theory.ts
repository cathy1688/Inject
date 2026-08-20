/**
 * Formula-only theoretical pool injection.
 *
 * Runtime logic in get_subnet_terms():
 *   alpha_in = tao_emission / alpha_price
 *   alpha_cap = root_proportion * alpha_emission
 *   tao_cap   = alpha_cap * alpha_price
 *   tao_in    = min(tao_emission, tao_cap)
 *
 * The theory module deliberately does not read or infer `SubnetTaoInEmission`.
 * The observed total TAO emission already collected for the block is used only
 * as the protocol upper bound. The pool split itself is calculated independently
 * from RootProp, AlphaEmission and Alpha Price.
 */
const FIXED_32 = 1n << 32n;
const PRICE_SCALE = 1_000_000_000n;

export interface TheoryInput {
  availableTaoEmissionRao: bigint;
  rootProportionRaw: bigint;
  alphaEmissionRao: bigint;
  priceRaoPerAlpha: bigint;
}

export function calculateInjectionCapRao(input: Omit<TheoryInput, 'availableTaoEmissionRao'>): bigint | null {
  // RootProp = 0 and AlphaEmission = 0 are valid protocol states. In either
  // case the theoretical pool-injection cap is exactly zero, not "missing".
  if (input.rootProportionRaw < 0n || input.alphaEmissionRao < 0n || input.priceRaoPerAlpha <= 0n) return null;

  // U96F32 has 32 fractional bits. Runtime price API is TAO/alpha scaled by 1e9.
  const alphaCapRao = (input.rootProportionRaw * input.alphaEmissionRao) / FIXED_32;
  return (alphaCapRao * input.priceRaoPerAlpha) / PRICE_SCALE;
}

export function calculateTheoreticalInjectedRao(input: TheoryInput): bigint | null {
  if (input.availableTaoEmissionRao < 0n) return null;
  const cap = calculateInjectionCapRao(input);
  if (cap == null) return null;
  return input.availableTaoEmissionRao < cap ? input.availableTaoEmissionRao : cap;
}
