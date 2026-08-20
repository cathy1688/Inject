import type { Env } from './types';
import { SubtensorRpc, parseBlockNumber } from './rpc';
import { storageKeys, decodeU64, decodeU96F32Raw } from './storage';
import { calculateTheoreticalInjectedRao } from './theory';

const DEFAULT_WS = 'wss://entrypoint-finney.opentensor.ai:443';

function json(data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data, (_key, value) => typeof value === 'bigint' ? value.toString() : value, 2), {
    status,
    headers: { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' }
  });
}

function toBigInt(value: unknown): bigint | null {
  if (typeof value === 'bigint') return value;
  if (typeof value === 'number' && Number.isSafeInteger(value) && value >= 0) return BigInt(value);
  if (typeof value === 'string' && /^\d+$/.test(value)) return BigInt(value);
  return null;
}

export async function theoryDebug(request: Request, env: Env): Promise<Response> {
  const url = new URL(request.url);
  const netuid = Number(url.searchParams.get('netuid') ?? '128');
  if (!Number.isInteger(netuid) || netuid <= 0 || netuid > 65535) return json({ error: 'invalid netuid' }, 400);

  const rpc = new SubtensorRpc(env.SUBTENSOR_WS_URL || DEFAULT_WS);
  try {
    await rpc.connect();
    const finalizedHash = await rpc.finalizedHead();
    const header = await rpc.header(finalizedHash);
    const blockNumber = parseBlockNumber(header.number);
    const parentHash = header.parentHash;

    const currentKeys = [
      storageKeys.taoInEmission(netuid),
      storageKeys.excessTao(netuid),
      storageKeys.alphaOutEmission(netuid)
    ];
    const parentKeys = [storageKeys.rootProp(netuid)];

    const [current, parent] = await Promise.all([
      rpc.queryStorage(currentKeys, finalizedHash),
      rpc.queryStorage(parentKeys, parentHash)
    ]);

    let priceRecords: unknown = null;
    let priceRpcError: string | null = null;
    try {
      priceRecords = await rpc.currentAlphaPricesAll(parentHash);
    } catch (error) {
      priceRpcError = error instanceof Error ? error.message : String(error);
    }

    const records = Array.isArray(priceRecords) ? priceRecords as Array<Record<string, unknown>> : [];
    const priceRecord = records.find(record => Number(record.netuid) === netuid) ?? null;
    const price = priceRecord ? toBigInt(priceRecord.price) : null;

    const actualHex = current.get(storageKeys.taoInEmission(netuid)) ?? null;
    const chainHex = current.get(storageKeys.excessTao(netuid)) ?? null;
    const alphaHex = current.get(storageKeys.alphaOutEmission(netuid)) ?? null;
    const rootHex = parent.get(storageKeys.rootProp(netuid)) ?? null;

    const actual = decodeU64(actualHex);
    const chainBuy = decodeU64(chainHex);
    const alphaEmission = decodeU64(alphaHex);
    const rootRaw = decodeU96F32Raw(rootHex);

    const theory = price && alphaEmission > 0n && rootRaw > 0n
      ? calculateTheoreticalInjectedRao({
          blockNumber,
          netuid,
          actualRao: actual,
          chainBuyRao: chainBuy,
          rootProportionRaw: rootRaw,
          alphaEmissionRao: alphaEmission,
          priceRaoPerAlpha: price
        })
      : null;

    return json({
      ok: true,
      netuid,
      blockNumber,
      finalizedHash,
      parentHash,
      keys: {
        taoInEmission: storageKeys.taoInEmission(netuid),
        excessTao: storageKeys.excessTao(netuid),
        alphaOutEmission: storageKeys.alphaOutEmission(netuid),
        rootProp: storageKeys.rootProp(netuid)
      },
      raw: { actualHex, chainHex, alphaHex, rootHex },
      decoded: {
        actualRao: actual,
        chainBuyRao: chainBuy,
        alphaEmissionRao: alphaEmission,
        rootProportionRaw: rootRaw,
        priceRaoPerAlpha: price,
        theoreticalInjectedRao: theory
      },
      price: {
        recordCount: records.length,
        matched: priceRecord,
        error: priceRpcError
      }
    });
  } catch (error) {
    return json({ ok: false, error: error instanceof Error ? error.message : String(error) }, 500);
  } finally {
    rpc.close();
  }
}
