import { Redis } from "@upstash/redis";
import { StoredShareV1, TrendPeriod, TrendResponse, TrendView } from "@/lib/share/types";
import { DEFAULT_SUBJECT_KIND, SubjectKind, parseSubjectKind } from "@/lib/subject-kind";

const SHARE_KEY_PREFIX = "share:";
const SHARE_IDS_KEY = "share:index:ids";
const SHARE_INDEX_CREATED_KEY = "share:index:created";
const SHARE_INDEX_UPDATED_KEY = "share:index:updated";
const TRENDS_CACHE_PREFIX = "trends:cache:";

const REDIS_URL = process.env.UPSTASH_REDIS_REST_URL;
const REDIS_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;
const REDIS_ENABLED = Boolean(REDIS_URL && REDIS_TOKEN);

let redisClient: Redis | null = null;

function getRedisClient(): Redis | null {
  if (!REDIS_ENABLED) {
    return null;
  }
  if (!redisClient) {
    redisClient = new Redis({
      url: REDIS_URL!,
      token: REDIS_TOKEN!,
      cache: "default",
      enableAutoPipelining: true,
    });
  }
  return redisClient;
}

type MemoryStore = {
  shares: Map<string, StoredShareV1>;
  trendCache: Map<string, { value: TrendResponse; expiresAt: number }>;
};

function normalizeStoredShare(input: StoredShareV1): StoredShareV1 {
  return {
    ...input,
    kind: parseSubjectKind(input.kind) ?? DEFAULT_SUBJECT_KIND,
  };
}

function getMemoryStore(): MemoryStore {
  const g = globalThis as typeof globalThis & {
    __MY9_SHARE_MEMORY__?: MemoryStore;
  };

  if (!g.__MY9_SHARE_MEMORY__) {
    g.__MY9_SHARE_MEMORY__ = {
      shares: new Map<string, StoredShareV1>(),
      trendCache: new Map<string, { value: TrendResponse; expiresAt: number }>(),
    };
  }
  return g.__MY9_SHARE_MEMORY__;
}

function trendCacheKey(period: TrendPeriod, view: TrendView, kind: SubjectKind) {
  return `${TRENDS_CACHE_PREFIX}${period}:${view}:${kind}`;
}

async function safeRedisGet<T>(key: string): Promise<T | null> {
  const redis = getRedisClient();
  if (!redis) {
    return null;
  }

  try {
    return (await redis.get<T>(key)) ?? null;
  } catch {
    return null;
  }
}

export async function saveShare(record: StoredShareV1): Promise<void> {
  const normalizedRecord = normalizeStoredShare(record);
  if (!REDIS_ENABLED) {
    getMemoryStore().shares.set(normalizedRecord.shareId, normalizedRecord);
    return;
  }

  const key = `${SHARE_KEY_PREFIX}${normalizedRecord.shareId}`;
  const redis = getRedisClient();
  if (!redis) {
    getMemoryStore().shares.set(normalizedRecord.shareId, normalizedRecord);
    return;
  }
  try {
    await redis.set(key, normalizedRecord);
    await redis.sadd(SHARE_IDS_KEY, normalizedRecord.shareId);
    await redis.zadd(SHARE_INDEX_CREATED_KEY, {
      score: normalizedRecord.createdAt,
      member: normalizedRecord.shareId,
    });
    await redis.zadd(SHARE_INDEX_UPDATED_KEY, {
      score: normalizedRecord.updatedAt,
      member: normalizedRecord.shareId,
    });
  } catch {
    getMemoryStore().shares.set(normalizedRecord.shareId, normalizedRecord);
  }
}

export async function getShare(shareId: string): Promise<StoredShareV1 | null> {
  if (!REDIS_ENABLED) {
    const fromMemory = getMemoryStore().shares.get(shareId);
    return fromMemory ? normalizeStoredShare(fromMemory) : null;
  }

  const key = `${SHARE_KEY_PREFIX}${shareId}`;
  const fromRedis = await safeRedisGet<StoredShareV1 | (Omit<StoredShareV1, "kind"> & { kind?: unknown })>(key);
  if (fromRedis) {
    return normalizeStoredShare(fromRedis as StoredShareV1);
  }
  const fromMemory = getMemoryStore().shares.get(shareId);
  return fromMemory ? normalizeStoredShare(fromMemory) : null;
}

export async function touchShare(shareId: string, now = Date.now()): Promise<boolean> {
  const existing = await getShare(shareId);
  if (!existing) {
    return false;
  }

  const updated: StoredShareV1 = {
    ...existing,
    updatedAt: now,
    lastViewedAt: now,
  };
  await saveShare(updated);
  return true;
}

async function getAllShareIdsFromRedis(): Promise<string[]> {
  const redis = getRedisClient();
  if (!redis) {
    return [];
  }

  try {
    const ids = await redis.smembers<string[]>(SHARE_IDS_KEY);
    if (Array.isArray(ids)) {
      return ids.map((id) => String(id));
    }
    return [];
  } catch {
    return [];
  }
}

export async function listAllShares(): Promise<StoredShareV1[]> {
  if (!REDIS_ENABLED) {
    return Array.from(getMemoryStore().shares.values()).map((item) =>
      normalizeStoredShare(item)
    );
  }

  const ids = await getAllShareIdsFromRedis();
  if (ids.length === 0) {
    return Array.from(getMemoryStore().shares.values()).map((item) =>
      normalizeStoredShare(item)
    );
  }

  const results: StoredShareV1[] = [];
  for (const shareId of ids) {
    const record = await safeRedisGet<StoredShareV1>(`${SHARE_KEY_PREFIX}${shareId}`);
    if (record) {
      results.push(normalizeStoredShare(record));
    }
  }
  return results;
}

function getPeriodStart(period: TrendPeriod, now = Date.now()): number {
  switch (period) {
    case "30d":
      return now - 30 * 24 * 60 * 60 * 1000;
    case "90d":
      return now - 90 * 24 * 60 * 60 * 1000;
    case "180d":
      return now - 180 * 24 * 60 * 60 * 1000;
    case "all":
    default:
      return 0;
  }
}

export async function listSharesByPeriod(period: TrendPeriod): Promise<StoredShareV1[]> {
  const all = await listAllShares();
  const from = getPeriodStart(period);
  return all.filter((item) => item.createdAt >= from);
}

export async function getTrendsCache(
  period: TrendPeriod,
  view: TrendView,
  kind: SubjectKind
): Promise<TrendResponse | null> {
  const key = trendCacheKey(period, view, kind);
  if (!REDIS_ENABLED) {
    const item = getMemoryStore().trendCache.get(key);
    if (!item) return null;
    if (Date.now() > item.expiresAt) {
      getMemoryStore().trendCache.delete(key);
      return null;
    }
    return item.value;
  }

  const data = await safeRedisGet<TrendResponse>(key);
  if (data) {
    return data;
  }

  const fallback = getMemoryStore().trendCache.get(key);
  if (!fallback) return null;
  if (Date.now() > fallback.expiresAt) {
    getMemoryStore().trendCache.delete(key);
    return null;
  }
  return fallback.value;
}

export async function setTrendsCache(
  period: TrendPeriod,
  view: TrendView,
  kind: SubjectKind,
  value: TrendResponse,
  ttlSeconds = 600
): Promise<void> {
  const key = trendCacheKey(period, view, kind);
  getMemoryStore().trendCache.set(key, {
    value,
    expiresAt: Date.now() + ttlSeconds * 1000,
  });

  if (!REDIS_ENABLED) {
    return;
  }

  const redis = getRedisClient();
  if (!redis) {
    return;
  }

  try {
    await redis.set(key, value, { ex: ttlSeconds });
  } catch {
    // ignore redis failures and keep in-memory cache
  }
}
