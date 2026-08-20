/**
 * Independent reconstruction of Subtensor pool-side TAO injection.
 *
 * No observed `SubnetTaoInEmission` or `SubnetExcessTao` value is used as a
 * theoretical input. For block N we reconstruct the runtime from the end-state
 * of block N-1:
 *
 *   global block emission(total issuance)
 *     -> EMA-price subnet shares
 *     -> miner-burn weighting
 *     -> emission gate (rank mode first, q-mass mode when rank=0)
 *     -> emission-enabled redistribution
 *     -> nominal subnet TAO emission
 *
 *   alpha emission(alpha issuance)
 *   root proportion(root TAO, TaoWeight, alpha issuance)
 *   spot alpha price
 *     -> injection cap = root_prop * alpha_emission * spot_price
 *
 *   theoretical_tao_in = min(nominal_subnet_tao_emission, injection_cap)
 *
 * The observed pool injection is compared only after this calculation.
 */
const FIXED_32 = 1n << 32n;
const FIXED_64 = 1n << 64n;
const U64_MAX = (1n << 64n) - 1n;
const TOTAL_SUPPLY_RAO = 21_000_000_000_000_000n;
const DEFAULT_BLOCK_EMISSION_RAO = 1_000_000_000n;
const PRICE_SCALE = 1_000_000_000n;

export const THEORY_MODEL_VERSION = '7-independent-rank';
export const DEFAULT_GATE_RANK = 32;
export const DEFAULT_GATE_EXPONENT_RAW = 3n * FIXED_64;
export const DEFAULT_GATE_QUANTILE_RAW = (61n * FIXED_64) / 100n;

export interface TheorySubnetState {
  netuid: number;
  networkAdded: boolean;
  firstEmissionStarted: boolean;
  subtokenEnabled: boolean;
  registrationAllowed: boolean;
  emissionEnabled: boolean;
  mechanism: number;
  movingPriceRaw: bigint; // I96F32 raw (32 fractional bits)
  minerBurnedRaw: bigint; // U96F32 raw (32 fractional bits)
  alphaInRao: bigint;
  alphaOutRao: bigint;
  priceRaoPerAlpha: bigint | null; // runtime price API, scaled by 1e9
}

export interface TheorySnapshot {
  totalIssuanceRao: bigint;
  rootTaoRao: bigint;
  taoWeightRaw: bigint; // u64 normalized by u64::MAX in runtime
  emissionGateBarRaw: bigint; // U64F64
  emissionGateExponentRaw: bigint; // U64F64
  emissionBarQuantileRaw: bigint; // U64F64
  emissionBarRank: number; // u16; rank mode when > 0
  subnets: TheorySubnetState[];
}

export interface TheorySubnetResult {
  nominalTaoEmissionRao: bigint;
  alphaEmissionRao: bigint;
  rootProportionRaw: bigint;
  theoreticalInjectedRao: bigint | null;
  missingPrice: boolean;
}

function fixed32ToNumber(raw: bigint): number {
  return Number(raw) / 4_294_967_296;
}

function fixed64ToNumber(raw: bigint): number {
  return Number(raw) / 18_446_744_073_709_551_616;
}

/** Runtime-equivalent halving schedule for TAO and per-subnet alpha issuance. */
export function blockEmissionForIssuanceRao(issuanceRao: bigint): bigint {
  if (issuanceRao < 0n || issuanceRao >= TOTAL_SUPPLY_RAO) return 0n;

  let emission = DEFAULT_BLOCK_EMISSION_RAO;
  let divisor = 2n;
  while (emission > 0n) {
    const threshold = TOTAL_SUPPLY_RAO - TOTAL_SUPPLY_RAO / divisor;
    if (issuanceRao < threshold) break;
    emission /= 2n;
    divisor *= 2n;
  }
  return emission;
}

/** Reconstruct `root_proportion()` without reading cached RootProp. */
export function calculateRootProportionRaw(
  rootTaoRao: bigint,
  taoWeightRaw: bigint,
  alphaIssuanceRao: bigint
): bigint {
  if (rootTaoRao <= 0n || taoWeightRaw <= 0n) return 0n;
  if (alphaIssuanceRao < 0n) return 0n;

  const taoWeightQ32 = (taoWeightRaw << 32n) / U64_MAX;
  const weightedRootRaw = rootTaoRao * taoWeightQ32;
  const alphaRaw = alphaIssuanceRao << 32n;
  const denominator = weightedRootRaw + alphaRaw;
  if (denominator <= 0n) return 0n;
  return (weightedRootRaw << 32n) / denominator;
}

export interface InjectionInput {
  nominalTaoEmissionRao: bigint;
  rootProportionRaw: bigint;
  alphaEmissionRao: bigint;
  priceRaoPerAlpha: bigint | null;
}

export function calculateInjectionCapRao(input: Omit<InjectionInput, 'nominalTaoEmissionRao'>): bigint | null {
  if (input.rootProportionRaw < 0n || input.alphaEmissionRao < 0n) return null;
  if (input.rootProportionRaw === 0n || input.alphaEmissionRao === 0n) return 0n;
  if (input.priceRaoPerAlpha == null || input.priceRaoPerAlpha <= 0n) return null;

  const alphaCapRao = (input.rootProportionRaw * input.alphaEmissionRao) / FIXED_32;
  return (alphaCapRao * input.priceRaoPerAlpha) / PRICE_SCALE;
}

export function calculateTheoreticalInjectedRao(input: InjectionInput): bigint | null {
  if (input.nominalTaoEmissionRao < 0n) return null;
  if (input.nominalTaoEmissionRao === 0n) return 0n;
  const cap = calculateInjectionCapRao(input);
  if (cap == null) return null;
  return input.nominalTaoEmissionRao < cap ? input.nominalTaoEmissionRao : cap;
}

function eligible(subnet: TheorySubnetState): boolean {
  return subnet.networkAdded && subnet.firstEmissionStarted && subnet.subtokenEnabled && subnet.registrationAllowed;
}

function normalizedPriceShares(subnets: TheorySubnetState[]): Map<number, number> {
  const prices = subnets.map(subnet => {
    const price = subnet.mechanism === 0 ? 1 : Math.max(0, fixed32ToNumber(subnet.movingPriceRaw));
    return [subnet.netuid, Number.isFinite(price) ? price : 0] as const;
  });
  const total = prices.reduce((sum, [, price]) => sum + price, 0);
  return new Map(prices.map(([netuid, price]) => [netuid, total > 0 ? price / total : 0]));
}

function minerBurnWeightedShares(subnets: TheorySubnetState[], priceShares: Map<number, number>): Map<number, number> {
  const weighted = subnets.map(subnet => {
    const burn = Math.min(1, Math.max(0, fixed32ToNumber(subnet.minerBurnedRaw)));
    return [subnet.netuid, (priceShares.get(subnet.netuid) ?? 0) * (1 - burn)] as const;
  });
  const total = weighted.reduce((sum, [, value]) => sum + value, 0);
  if (!(total > 0)) return new Map(priceShares);
  return new Map(weighted.map(([netuid, value]) => [netuid, value / total]));
}

/** Mirror `maybe_update_emission_gate_bar`: rank mode wins when rank > 0. */
function selectGateBar(shares: Map<number, number>, rank: number, quantile: number): number {
  const positive = [...shares.values()].filter(value => value > 0).sort((a, b) => b - a);
  if (!positive.length) return 0;
  if (rank > 0) return positive[Math.min(rank, positive.length) - 1];

  let cumulative = 0;
  for (const share of positive) {
    cumulative += share;
    if (cumulative >= quantile) return share;
  }
  return positive[positive.length - 1];
}

function gatedShares(
  weighted: Map<number, number>,
  blockNumber: number,
  storedBar: number,
  rank: number,
  quantile: number,
  exponent: number
): Map<number, number> {
  const theta = storedBar > 0 && blockNumber % 360 !== 0
    ? storedBar
    : selectGateBar(weighted, rank, quantile);
  if (!(theta > 0)) return new Map(weighted);

  const gated = new Map<number, number>();
  let total = 0;
  for (const [netuid, share] of weighted) {
    if (!(share > 0)) {
      gated.set(netuid, 0);
      continue;
    }
    const ratio = theta / share;
    const powered = Math.pow(ratio, exponent);
    const gate = 1 / (1 + powered);
    const value = Number.isFinite(gate) ? share * gate : 0;
    gated.set(netuid, value);
    total += value;
  }
  if (!(total > 0)) return new Map(weighted);
  for (const [netuid, value] of gated) gated.set(netuid, value / total);
  return gated;
}

function redistributeDisabled(subnets: TheorySubnetState[], shares: Map<number, number>): Map<number, number> {
  const hasDisabled = subnets.some(subnet => !subnet.emissionEnabled);
  if (!hasDisabled) return new Map(shares);
  const enabledTotal = subnets.reduce(
    (sum, subnet) => sum + (subnet.emissionEnabled ? (shares.get(subnet.netuid) ?? 0) : 0),
    0
  );
  const out = new Map<number, number>();
  for (const subnet of subnets) {
    out.set(subnet.netuid, subnet.emissionEnabled && enabledTotal > 0
      ? (shares.get(subnet.netuid) ?? 0) / enabledTotal
      : 0);
  }
  return out;
}

/** Fully independent per-subnet theoretical terms for block `blockNumber`. */
export function calculateIndependentTheory(snapshot: TheorySnapshot, blockNumber: number): Map<number, TheorySubnetResult> {
  const emitting = snapshot.subnets.filter(eligible);
  const priceShares = normalizedPriceShares(emitting);
  const weightedShares = minerBurnWeightedShares(emitting, priceShares);
  const bar = Math.max(0, fixed64ToNumber(snapshot.emissionGateBarRaw));
  const exponentRaw = snapshot.emissionGateExponentRaw > 0n ? snapshot.emissionGateExponentRaw : DEFAULT_GATE_EXPONENT_RAW;
  const quantileRaw = snapshot.emissionBarQuantileRaw > 0n ? snapshot.emissionBarQuantileRaw : DEFAULT_GATE_QUANTILE_RAW;
  const exponent = Math.max(0, fixed64ToNumber(exponentRaw));
  const quantile = Math.min(1, Math.max(0, fixed64ToNumber(quantileRaw)));
  const rank = Number.isInteger(snapshot.emissionBarRank) && snapshot.emissionBarRank >= 0
    ? snapshot.emissionBarRank
    : DEFAULT_GATE_RANK;
  const afterGate = gatedShares(weightedShares, blockNumber, bar, rank, quantile, exponent);
  const effectiveShares = redistributeDisabled(emitting, afterGate);
  const globalBlockEmissionRao = blockEmissionForIssuanceRao(snapshot.totalIssuanceRao);

  const nominalByNetuid = new Map<number, bigint>();
  for (const subnet of snapshot.subnets) nominalByNetuid.set(subnet.netuid, 0n);
  for (const subnet of emitting) {
    const share = effectiveShares.get(subnet.netuid) ?? 0;
    const emission = Math.max(0, Math.floor(Number(globalBlockEmissionRao) * share));
    nominalByNetuid.set(subnet.netuid, BigInt(emission));
  }

  const result = new Map<number, TheorySubnetResult>();
  for (const subnet of snapshot.subnets) {
    const alphaIssuanceRao = subnet.alphaInRao + subnet.alphaOutRao;
    const alphaEmissionRao = blockEmissionForIssuanceRao(alphaIssuanceRao);
    const rootProportionRaw = calculateRootProportionRaw(snapshot.rootTaoRao, snapshot.taoWeightRaw, alphaIssuanceRao);
    const nominalTaoEmissionRao = nominalByNetuid.get(subnet.netuid) ?? 0n;
    const theoreticalInjectedRao = calculateTheoreticalInjectedRao({
      nominalTaoEmissionRao,
      rootProportionRaw,
      alphaEmissionRao,
      priceRaoPerAlpha: subnet.priceRaoPerAlpha
    });
    result.set(subnet.netuid, {
      nominalTaoEmissionRao,
      alphaEmissionRao,
      rootProportionRaw,
      theoreticalInjectedRao,
      missingPrice: theoreticalInjectedRao == null && nominalTaoEmissionRao > 0n && rootProportionRaw > 0n && alphaEmissionRao > 0n
    });
  }
  return result;
}
