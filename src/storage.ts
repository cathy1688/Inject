// Precomputed Substrate Twox128 prefixes. Keeping them static avoids CPU-heavy
// hash work inside the Workers Free CPU budget.
const SUBTENSOR = '658faa385070e074c85bf6b568cf0555';
const TIMESTAMP = 'f0c365c3cf59d671eb72da0e7a4113c4';

const ITEMS = {
  networksAdded: '0e30450fc4d507a846032a7fa65d9a43',
  taoInEmission: 'dd62ae7237581e8f6a684f1ecae06215',
  excessTao: '857b0a5b920bc5e41cb0695a4b7d38e7',
  registeredSubnetCounter: '29b0b4bcda192c1cf596d879a4e873a6',
  subnetIdentitiesV3: 'c7cb9786b286b680ca204a6c0920ee52',
  timestampNow: '9f1f0515f462cdcf84e0f1d6045dfcbb'
} as const;

export const PREFIX = {
  networksAdded: `0x${SUBTENSOR}${ITEMS.networksAdded}`,
  subnetIdentitiesV3: `0x${SUBTENSOR}${ITEMS.subnetIdentitiesV3}`
} as const;

export const TIMESTAMP_NOW_KEY = `0x${TIMESTAMP}${ITEMS.timestampNow}`;

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
  registeredSubnetCounter: (netuid: number) => identityMapKey(ITEMS.registeredSubnetCounter, netuid)
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
  // Blake2_128Concat appends the original SCALE-encoded key after 16 hash bytes.
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

export function decodeU64(hexValue: string | null): bigint {
  const bytes = hexToBytes(hexValue);
  let value = 0n;
  const limit = Math.min(bytes.length, 8);
  for (let i = 0; i < limit; i++) value |= BigInt(bytes[i]) << BigInt(i * 8);
  return value;
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
