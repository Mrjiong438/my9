#!/usr/bin/env node

import { neon } from "@neondatabase/serverless";
import { resolve } from "node:path";

const SHARES_V2_TABLE = "my9_share_registry_v2";
const TREND_COUNT_ALL_TABLE = "my9_trend_subject_kind_all_v3";
const TREND_COUNT_DAY_TABLE = "my9_trend_subject_kind_day_v3";
const TREND_COUNT_HOUR_TABLE = "my9_trend_subject_kind_hour_v3";
const TRENDS_CACHE_TABLE = "my9_trends_cache_v1";
const TREND_COUNT_ALL_KIND_COUNT_IDX = `${TREND_COUNT_ALL_TABLE}_kind_count_idx`;
const TRENDS_CACHE_EXPIRES_IDX = `${TRENDS_CACHE_TABLE}_expires_idx`;

const HOUR_MS = 60 * 60 * 1000;
const BEIJING_TZ_OFFSET_MS = 8 * 60 * 60 * 1000;
const DEFAULT_MAX_ATTEMPTS = 12;
const DEFAULT_LOCK_TIMEOUT_MS = 5000;

function loadLocalEnvFiles() {
  for (const file of [".env.local", ".env"]) {
    try {
      process.loadEnvFile(resolve(process.cwd(), file));
    } catch {
      // ignore missing env file
    }
  }
}

function readEnv(...names) {
  for (const name of names) {
    const value = process.env[name];
    if (typeof value === "string" && value.trim()) {
      return value.trim();
    }
  }
  return null;
}

function buildDatabaseUrlFromNeonParts() {
  const host = readEnv("NEON_DATABASE_PGHOST_UNPOOLED", "NEON_DATABASE_PGHOST");
  const user = readEnv("NEON_DATABASE_PGUSER");
  const password = readEnv("NEON_DATABASE_PGPASSWORD", "NEON_DATABASE_POSTGRES_PASSWORD");
  const database = readEnv("NEON_DATABASE_PGDATABASE", "NEON_DATABASE_POSTGRES_DATABASE");
  if (!host || !user || !password || !database) {
    return null;
  }

  let hostWithPort = host;
  const port = readEnv("NEON_DATABASE_PGPORT");
  if (port && !host.includes(":")) {
    hostWithPort = `${host}:${port}`;
  }

  const sslMode = readEnv("NEON_DATABASE_PGSSLMODE") ?? "require";
  return `postgresql://${encodeURIComponent(user)}:${encodeURIComponent(
    password
  )}@${hostWithPort}/${encodeURIComponent(database)}?sslmode=${encodeURIComponent(sslMode)}`;
}

function parseArg(name, fallback) {
  const prefix = `--${name}=`;
  const withEquals = process.argv.find((arg) => arg.startsWith(prefix));
  if (withEquals) {
    const parsed = Number(withEquals.slice(prefix.length));
    return Number.isFinite(parsed) ? Math.trunc(parsed) : fallback;
  }

  const index = process.argv.indexOf(`--${name}`);
  if (index === -1 || index + 1 >= process.argv.length) return fallback;
  const parsed = Number(process.argv[index + 1]);
  return Number.isFinite(parsed) ? Math.trunc(parsed) : fallback;
}

function sleep(ms) {
  return new Promise((resolveSleep) => {
    setTimeout(resolveSleep, ms);
  });
}

function isRetryableError(error) {
  if (!(error instanceof Error)) return false;
  const message = error.message.toLowerCase();
  return (
    message.includes("deadlock detected") ||
    message.includes("lock timeout") ||
    message.includes("canceling statement due to lock timeout") ||
    message.includes("could not obtain lock")
  );
}

async function ensureTables(sql) {
  await sql.query(
    `
    CREATE TABLE IF NOT EXISTS ${TREND_COUNT_ALL_TABLE} (
      kind TEXT NOT NULL,
      subject_id TEXT NOT NULL,
      count BIGINT NOT NULL,
      updated_at BIGINT NOT NULL,
      PRIMARY KEY (kind, subject_id)
    )
    `
  );

  await sql.query(
    `
    CREATE INDEX IF NOT EXISTS ${TREND_COUNT_ALL_KIND_COUNT_IDX}
    ON ${TREND_COUNT_ALL_TABLE} (kind, count DESC, subject_id)
    `
  );

  await sql.query(
    `
    CREATE TABLE IF NOT EXISTS ${TREND_COUNT_DAY_TABLE} (
      kind TEXT NOT NULL,
      day_key INT NOT NULL,
      subject_id TEXT NOT NULL,
      count BIGINT NOT NULL,
      updated_at BIGINT NOT NULL,
      PRIMARY KEY (kind, day_key, subject_id)
    )
    `
  );

  await sql.query(
    `
    CREATE TABLE IF NOT EXISTS ${TREND_COUNT_HOUR_TABLE} (
      kind TEXT NOT NULL,
      hour_bucket BIGINT NOT NULL,
      subject_id TEXT NOT NULL,
      count BIGINT NOT NULL,
      updated_at BIGINT NOT NULL,
      PRIMARY KEY (kind, hour_bucket, subject_id)
    )
    `
  );

  await sql.query(
    `
    CREATE TABLE IF NOT EXISTS ${TRENDS_CACHE_TABLE} (
      cache_key TEXT PRIMARY KEY,
      period TEXT NOT NULL,
      view TEXT NOT NULL,
      kind TEXT NOT NULL,
      payload JSONB NOT NULL,
      expires_at BIGINT NOT NULL,
      updated_at BIGINT NOT NULL
    )
    `
  );

  await sql.query(
    `
    CREATE INDEX IF NOT EXISTS ${TRENDS_CACHE_EXPIRES_IDX}
    ON ${TRENDS_CACHE_TABLE} (expires_at)
    `
  );
}

async function rebuildFromRegistry(sql, params) {
  const { nowMs, lockTimeoutMs, maxAttempts } = params;
  let attempt = 0;

  while (attempt < maxAttempts) {
    attempt += 1;
    try {
      await sql.transaction((txn) => [
        txn.query(`SET LOCAL lock_timeout = '${lockTimeoutMs}ms'`),
        txn.query(
          `
          LOCK TABLE
            ${TREND_COUNT_ALL_TABLE},
            ${TREND_COUNT_DAY_TABLE},
            ${TREND_COUNT_HOUR_TABLE},
            ${TRENDS_CACHE_TABLE}
          IN ACCESS EXCLUSIVE MODE
          `
        ),
        txn.query(
          `
          TRUNCATE TABLE
            ${TREND_COUNT_ALL_TABLE},
            ${TREND_COUNT_DAY_TABLE},
            ${TREND_COUNT_HOUR_TABLE},
            ${TRENDS_CACHE_TABLE}
          `
        ),
        txn.query(
          `
          WITH share_slots AS (
            SELECT
              s.kind,
              NULLIF(BTRIM(slot.value->>'sid'), '') AS subject_id
            FROM ${SHARES_V2_TABLE} s
            CROSS JOIN LATERAL jsonb_array_elements(
              CASE
                WHEN s.hot_payload IS NULL THEN '[]'::jsonb
                WHEN jsonb_typeof(s.hot_payload) = 'array' THEN s.hot_payload
                ELSE '[]'::jsonb
              END
            ) AS slot(value)
            WHERE s.hot_payload IS NOT NULL
          ),
          folded AS (
            SELECT
              kind,
              subject_id,
              COUNT(*)::BIGINT AS count
            FROM share_slots
            WHERE subject_id IS NOT NULL
            GROUP BY kind, subject_id
          )
          INSERT INTO ${TREND_COUNT_ALL_TABLE} (kind, subject_id, count, updated_at)
          SELECT kind, subject_id, count, $1::BIGINT
          FROM folded
          ON CONFLICT (kind, subject_id) DO UPDATE SET
            count = EXCLUDED.count,
            updated_at = EXCLUDED.updated_at
          `,
          [nowMs]
        ),
        txn.query(
          `
          WITH share_slots AS (
            SELECT
              s.kind,
              TO_CHAR(
                timezone('Asia/Shanghai', to_timestamp(s.created_at / 1000.0)),
                'YYYYMMDD'
              )::INT AS day_key,
              NULLIF(BTRIM(slot.value->>'sid'), '') AS subject_id
            FROM ${SHARES_V2_TABLE} s
            CROSS JOIN LATERAL jsonb_array_elements(
              CASE
                WHEN s.hot_payload IS NULL THEN '[]'::jsonb
                WHEN jsonb_typeof(s.hot_payload) = 'array' THEN s.hot_payload
                ELSE '[]'::jsonb
              END
            ) AS slot(value)
            WHERE s.hot_payload IS NOT NULL
          ),
          folded AS (
            SELECT
              kind,
              day_key,
              subject_id,
              COUNT(*)::BIGINT AS count
            FROM share_slots
            WHERE subject_id IS NOT NULL
            GROUP BY kind, day_key, subject_id
          )
          INSERT INTO ${TREND_COUNT_DAY_TABLE} (kind, day_key, subject_id, count, updated_at)
          SELECT kind, day_key, subject_id, count, $1::BIGINT
          FROM folded
          ON CONFLICT (kind, day_key, subject_id) DO UPDATE SET
            count = EXCLUDED.count,
            updated_at = EXCLUDED.updated_at
          `,
          [nowMs]
        ),
        txn.query(
          `
          WITH share_slots AS (
            SELECT
              s.kind,
              FLOOR((s.created_at + ${BEIJING_TZ_OFFSET_MS}) / ${HOUR_MS})::BIGINT AS hour_bucket,
              NULLIF(BTRIM(slot.value->>'sid'), '') AS subject_id
            FROM ${SHARES_V2_TABLE} s
            CROSS JOIN LATERAL jsonb_array_elements(
              CASE
                WHEN s.hot_payload IS NULL THEN '[]'::jsonb
                WHEN jsonb_typeof(s.hot_payload) = 'array' THEN s.hot_payload
                ELSE '[]'::jsonb
              END
            ) AS slot(value)
            WHERE s.hot_payload IS NOT NULL
          ),
          folded AS (
            SELECT
              kind,
              hour_bucket,
              subject_id,
              COUNT(*)::BIGINT AS count
            FROM share_slots
            WHERE subject_id IS NOT NULL
            GROUP BY kind, hour_bucket, subject_id
          )
          INSERT INTO ${TREND_COUNT_HOUR_TABLE} (kind, hour_bucket, subject_id, count, updated_at)
          SELECT kind, hour_bucket, subject_id, count, $1::BIGINT
          FROM folded
          ON CONFLICT (kind, hour_bucket, subject_id) DO UPDATE SET
            count = EXCLUDED.count,
            updated_at = EXCLUDED.updated_at
          `,
          [nowMs]
        ),
      ]);
      return attempt;
    } catch (error) {
      if (!isRetryableError(error) || attempt >= maxAttempts) {
        throw error;
      }
      const backoffMs = Math.min(4000, 200 * 2 ** (attempt - 1));
      await sleep(backoffMs);
    }
  }

  throw new Error("rebuild exhausted retries");
}

async function main() {
  loadLocalEnvFiles();

  const nowMs = parseArg("now-ms", Date.now());
  const maxAttempts = parseArg("max-attempts", DEFAULT_MAX_ATTEMPTS);
  const lockTimeoutMs = parseArg("lock-timeout-ms", DEFAULT_LOCK_TIMEOUT_MS);
  const databaseUrl = process.env.DATABASE_URL || buildDatabaseUrlFromNeonParts();
  if (!databaseUrl) {
    throw new Error("DATABASE_URL / NEON_DATABASE_* is required");
  }

  const sql = neon(databaseUrl);
  await ensureTables(sql);
  const applyAttempt = await rebuildFromRegistry(sql, {
    nowMs,
    lockTimeoutMs,
    maxAttempts,
  });

  await sql.query(`ANALYZE ${TREND_COUNT_ALL_TABLE}`);
  await sql.query(`ANALYZE ${TREND_COUNT_DAY_TABLE}`);
  await sql.query(`ANALYZE ${TREND_COUNT_HOUR_TABLE}`);

  const sourceRows = await sql.query(
    `
    SELECT
      COUNT(*)::BIGINT AS total_shares,
      COUNT(*) FILTER (WHERE hot_payload IS NOT NULL)::BIGINT AS hot_shares
    FROM ${SHARES_V2_TABLE}
    `
  );
  const allRows = await sql.query(`SELECT COUNT(*)::BIGINT AS total FROM ${TREND_COUNT_ALL_TABLE}`);
  const dayRows = await sql.query(`SELECT COUNT(*)::BIGINT AS total FROM ${TREND_COUNT_DAY_TABLE}`);
  const hourRows = await sql.query(`SELECT COUNT(*)::BIGINT AS total FROM ${TREND_COUNT_HOUR_TABLE}`);
  const cacheRows = await sql.query(`SELECT COUNT(*)::BIGINT AS total FROM ${TRENDS_CACHE_TABLE}`);

  console.log(
    JSON.stringify(
      {
        ok: true,
        nowMs,
        lockTimeoutMs,
        maxAttempts,
        applyAttempt,
        source: {
          totalShares: Number(sourceRows[0]?.total_shares ?? 0),
          hotShares: Number(sourceRows[0]?.hot_shares ?? 0),
        },
        rebuilt: {
          allRows: Number(allRows[0]?.total ?? 0),
          dayRows: Number(dayRows[0]?.total ?? 0),
          hourRows: Number(hourRows[0]?.total ?? 0),
          cacheRows: Number(cacheRows[0]?.total ?? 0),
        },
      },
      null,
      2
    )
  );
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
