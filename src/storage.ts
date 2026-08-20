// Precomputed Substrate Twox128 prefixes. Keeping them static avoids CPU-heavy
// hash work inside the Workers Free CPU budget.
const SUBTENSOR = '658faa385070e074c85bf6b568cf0555';
const TIMESTAMP = 'f0c365c3cf59d671eb72da0e7a4113c4';

const ITEMS = {
  networksAdded: '0e30450fc4d507a846032a7fa65d9a43',
  taoInEmission: 'dd62ae7237581e8f6a684f1ecae06215',
  excessTao: '857b0a5b920bc5e41cb0695a4b7d38e7',
  alphaOutEmission: '25257fbc5458419b7bc7e8c44c521521',
  rootProp: '010cf3b550f933211beccf08e00433e4',
  registeredSubnetCounter: '29b0b4bcda192c1cf596d879a4e873a6',
  subnetIdentitiesV3: 'c7cb9786b286b680ca204a6c0920ee52',

  // Independent emission reconstruction inputs (runtime spec 447).
  subnetMovingPrice: '1abf1b0f4fd14f7b72ee50f9d91d5915',
  minerBurned: '1eac6222ebba7feba4ca36a94736815e',
  subnetEmissionEnabled: 'c97bb5c5631e5f593b5bd2da84a5fa16',
  firstEmissionBlockNumber: 'e4cfee4e36f2419d8863a3fda65c428f',
  subtokenEnabled: 'e9348e9224ea06c9c2da12ce69e619c5',
  networkRegistrationAllowed: 'd5fe74da02c7b4bbb340fb368eee3e77',
  subnetMechanism: '306afce653cf1dfd6333a3c30d8d347e',
  subnetTao: '7a57dce016211512d1700561066b85a3',
  subnetAlphaIn: '2ce12f7007574647d692ac7edf8b7a53',
  subnetAlphaOut: '7837978cc6746112a2c9e680a18cfcb9',

  // Global independent emission inputs.
  totalIssuance: '57c875e4cff74148e4628f264b974c80',
  taoWeight: '6b2684762c3b1e22ffb4a92939298741',
  emissionGateBar: '7c9b0d2964cc73e7519676c3cc4d5df9',
  emissionGateExponent: '88c70e8dd0cf4af3aeb977ba2eee1df4',
  emissionBarQuantile: 'a772007dde2ed63e0f21b5f9d7f16650',

  timestampNow: '9f1f0515f462cdcf84e0f1d6045dfcbb'
} as const;

export const PREFIX = {
  networksAdded: `0x${SUBTENSOR}${ITEMS.networksAdded}`,
  subnetIdentitiesV3: `0x${SUBTENSOR}${ITEMS.subnetIdentitiesV3}`
} as const;

export const TIMESTAMP_NOW_KEY = `0x${TIMESTAMP}${ITEMS.timestampNow}`;

export const globalStorageKeys = {
  totalIssuance: `0x${SUBTENSOR}${ITEMS.totalIssuance}`,
  taoWeight: `0x${SUBTENSOR}${ITEMS.taoWeight}`,
  emissionGateBar: `0x${SUBTENSOR}${ITEMS.emissionGateBar}`,
  emissionGateExponent: `0x${SUBTENSOR}${ITEMS.emissionGateExponent}`,
  emissionBarQuantile: `0x${SUBTENSOR}${ITEMS.emissionBarQuantile}`
} as const;

function u16LeHex(value: number): string {
  const n = value & 0xffff;
  return (n & 0xff).toString(16).padStart(2, '0') + ((n >>> 8) & 0xff).toString(16).padStart(2, '0');
}

function identityMapKey(itemHash: string, netuid: number): string {
  return `0x${SUBTENSOR}${itemHash}${u16LeHex(netuid)}`;
}

export const storageKeys = {
  networksAdded: (netuid: number) => identityMapKey(ITEMS.networksAdded, netuid),
  taoInEmission: (netuid: number) => identityMapKey(ITEMS.taoInEmission, netuid),
  excessTao: (netuid: number) => identityMapKey(ITEMS.excessTao, netuid),
  alphaOutEmission: (netuid: number) => identityMapKey(ITEMS.alphaOutEmission, netuid),
  rootProp: (netuid: number) => identityMapKey(ITEMS.rootProp, netuid),
  registeredSubnetCounter: (netuid: number) => identityMapKey(ITEMS.registeredSubnetCounter, netuid),

  subnetMovingPrice: (netuid: number) => identityMapKey(ITEMS.subnetMovingPrice, netuid),
  minerBurned: (netuid: number) => identityMapKey(ITEMS.minerBurned, netuid),
  subnetEmissionEnabled: (netuid: number) => identityMapKey(ITEMS.subnetEmissionEnabled, netuid),
  firstEmissionBlockNumber: (netuid: number) => identityMapKey(ITEMS.firstEmissionBlockNumber, netuid),
  subtokenEnabled: (netuid: number) => identityMapKey(ITEMS.subtokenEnabled, netuid),
  networkRegistrationAllowed: (netuid: number) => identityMapKey(ITEMS.networkRegistrationAllowed, netuid),
  subnetMechanism: (netuid: number) => identityMapKey(ITEMS.subnetMechanism, netuid),
  subnetTao: (netuid: number) => identityMapKey(ITEMS.subnetTao, netuid),
  subnetAlphaIn: (netuid: number) => identityMapKey(ITEMS.subnetAlphaIn, netuid),
  subnetAlphaOut: (netuid: number) => identityMapKey(ITEMS.subnetAlphaOut, netuid)
};

export function netuidFromIdentityStorageKey(key: string): number | null {
  const hex = key.startsWith('0x') ? key.slice(2) : key;
  if (hex.length < 4) return null;
  const lo = Number.parseInt(hex.slice(-4, -2), 16);
  const hi = Number.parseInt(hex.slice(-2), 16);
  if (!Number.isFinite(lo) || !Number.isFinite(hi)) return null;
  return lo | (hi << 8);
}

export function netuidFromBlake2ConcatStorageKey(key: string): number | null {
  return netuidFromIdentityStorageKey(key);
}

export function hexToBytes(hexValue: string | null): Uint8Array {
  if (!hexValue) return new Uint8Array();
  const hex = hexValue.startsWith('0x') ? hexValue.slice(2) : hexValue;
  if (!hex.length) return new Uint8Array();
  const out = new Uint8Array(Math.floor(hex.length / 2));
  for (let i = 0; i < out.length; i++) out[i] = Number.parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  return out;
}

export function decodeBool(hexValue: string | null): boolean {
  const bytes = hexToBytes(hexValue);
  return bytes.length > 0 && bytes[0] !== 0;
}

function decodeUnsignedLe(bytes: Uint8Array, maxBytes: number): bigint {
  let value = 0n;
  const limit = Math.min(bytes.length, maxBytes);
  for (let i = 0; i < limit; i++) value |= BigInt(bytes[i]) << BigInt(i * 8);
  return value;
}

export function decodeU16(hexValue: string | null): number {
  return Number(decodeUnsignedLe(hexToBytes(hexValue), 2));
}

export function decodeU64(hexValue: string | null): bigint {
  return decodeUnsignedLe(hexToBytes(hexValue), 8);
}

export function decodeU128(hexValue: string | null): bigint {
  return decodeUnsignedLe(hexToBytes(hexValue), 16);
}

/** U96F32 is stored as a 128-bit little-endian fixed point value with 32 fractional bits. */
export function decodeU96F32Raw(hexValue: string | null): bigint {
  return decodeU128(hexValue);
}

/** I96F32 is signed 128-bit little-endian with 32 fractional bits. */
export function decodeI96F32Raw(hexValue: string | null): bigint {
  const unsigned = decodeU128(hexValue);
  const sign = 1n << 127n;
  return (unsigned & sign) === 0n ? unsigned : unsigned - (1n << 128n);
}

/** U64F64 is stored as a 128-bit little-endian fixed point value with 64 fractional bits. */
export function decodeU64F64Raw(hexValue: string | null): bigint {
  return decodeU128(hexValue);
}

function decodeCompact(bytes: Uint8Array, offset = 0): { value: number; next: number } | null {
  if (offset >= bytes.length) return null;
  const first = bytes[offset];
  const mode = first & 0b11;
  if (mode === 0) return { value: first >>> 2, next: offset + 1 };
  if (mode === 1) {
    if (offset + 1 >= bytes.length) return null;
    return { value: ((first >>> 2) | (bytes[offset + 1] << 6)) >>> 0, next: offset + 2 };
  }
  if (mode === 2) {
    if (offset + 3 >= bytes.length) return null;
    const raw = (bytes[offset] | (bytes[offset + 1] << 8) | (bytes[offset + 2] << 16) | (bytes[offset + 3] << 24)) >>> 0;
    return { value: raw >>> 2, next: offset + 4 };
  }
  const byteLen = (first >>> 2) + 4;
  if (byteLen > 4 || offset + 1 + byteLen > bytes.length) return null;
  let value = 0;
  for (let i = 0; i < byteLen; i++) value += bytes[offset + 1 + i] * 2 ** (8 * i);
  return { value, next: offset + 1 + byteLen };
}

/** Decode SCALE Vec<SubnetPrice>, where SubnetPrice = { netuid: u16, price: u64 }. */
export function decodeSubnetPrices(hexValue: string | null): Map<number, bigint> {
  const bytes = hexToBytes(hexValue);
  const length = decodeCompact(bytes, 0);
  const prices = new Map<number, bigint>();
  if (!length) return prices;
  let offset = length.next;
  for (let i = 0; i < length.value; i++) {
    if (offset + 10 > bytes.length) break;
    const netuid = bytes[offset] | (bytes[offset + 1] << 8);
    offset += 2;
    const price = decodeUnsignedLe(bytes.slice(offset, offset + 8), 8);
    offset += 8;
    prices.set(netuid, price);
  }
  return prices;
}

export function decodeFirstVecUtf8(hexValue: string | null): string | null {
  const bytes = hexToBytes(hexValue);
  const length = decodeCompact(bytes, 0);
  if (!length || length.value < 0 || length.next + length.value > bytes.length) return null;
  try {
    const text = new TextDecoder('utf-8', { fatal: false }).decode(bytes.slice(length.next, length.next + length.value)).trim();
    return text || null;
  } catch {
    return null;
  }
}
