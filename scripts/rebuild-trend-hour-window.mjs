#!/usr/bin/env node

import { neon } from "@neondatabase/serverless";
import { resolve } from "node:path";

const SHARES_V2_TABLE = "my9_share_registry_v2";
const TREND_COUNT_HOUR_TABLE = "my9_trend_subject_hour_v1";
const TREND_COUNT_HOUR_STAGE_TABLE = "my9_trend_subject_hour_rebuild_stage_v1";

const HOUR_MS = 60 * 60 * 1000;
const DAY_MS = 24 * 60 * 60 * 1000;
const BEIJING_TZ_OFFSET_MS = 8 * 60 * 60 * 1000;
const DEFAULT_APPLY_MAX_ATTEMPTS = 12;
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
  if (!host || !user || !password || !database) return null;

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

function getBeijingDayStart(timestampMs) {
  return Math.floor((timestampMs + BEIJING_TZ_OFFSET_MS) / DAY_MS) * DAY_MS - BEIJING_TZ_OFFSET_MS;
}

function toBeijingHourBucket(timestampMs) {
  return Math.floor((timestampMs + BEIJING_TZ_OFFSET_MS) / HOUR_MS);
}

function sleep(ms) {
  return new Promise((resolveSleep) => {
    setTimeout(resolveSleep, ms);
  });
}

function isRetryableApplyError(error) {
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
    CREATE TABLE IF NOT EXISTS ${TREND_COUNT_HOUR_TABLE} (
      hour_bucket BIGINT NOT NULL,
      subject_id TEXT NOT NULL,
      count BIGINT NOT NULL,
      updated_at BIGINT NOT NULL,
      PRIMARY KEY (hour_bucket, subject_id)
    )
    `
  );

  await sql.query(
    `
    CREATE TABLE IF NOT EXISTS ${TREND_COUNT_HOUR_STAGE_TABLE} (
      run_id TEXT NOT NULL,
      hour_bucket BIGINT NOT NULL,
      subject_id TEXT NOT NULL,
      count BIGINT NOT NULL,
      created_at BIGINT NOT NULL,
      PRIMARY KEY (run_id, hour_bucket, subject_id)
    )
    `
  );

  await sql.query(
    `
    CREATE INDEX IF NOT EXISTS ${TREND_COUNT_HOUR_STAGE_TABLE}_created_idx
    ON ${TREND_COUNT_HOUR_STAGE_TABLE} (created_at)
    `
  );
}

async function stageWindowCounts(sql, params) {
  const { runId, windowStartMs, nowMs } = params;
  const stageRetentionCutoff = nowMs - 2 * DAY_MS;

  await sql.query(
    `
    DELETE FROM ${TREND_COUNT_HOUR_STAGE_TABLE}
    WHERE created_at < $1
    `,
    [stageRetentionCutoff]
  );

  await sql.query(
    `
    DELETE FROM ${TREND_COUNT_HOUR_STAGE_TABLE}
    WHERE run_id = $1
    `,
    [runId]
  );

  const rows = await sql.query(
    `
    WITH share_slots AS (
      SELECT
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
      WHERE s.created_at >= $1
        AND s.created_at <= $2
    ),
    folded AS (
      SELECT
        hour_bucket,
        subject_id,
        COUNT(*)::BIGINT AS count
      FROM share_slots
      WHERE subject_id IS NOT NULL
      GROUP BY hour_bucket, subject_id
    ),
    inserted AS (
      INSERT INTO ${TREND_COUNT_HOUR_STAGE_TABLE} (run_id, hour_bucket, subject_id, count, created_at)
      SELECT
        $3::TEXT AS run_id,
        hour_bucket,
        subject_id,
        count,
        $4::BIGINT AS created_at
      FROM folded
      ON CONFLICT (run_id, hour_bucket, subject_id) DO UPDATE SET
        count = EXCLUDED.count,
        created_at = EXCLUDED.created_at
      RETURNING 1
    )
    SELECT COUNT(*)::BIGINT AS row_count
    FROM inserted
    `,
    [windowStartMs, nowMs, runId, nowMs]
  );

  return Number(rows[0]?.row_count ?? 0);
}

async function applyStagedWindowWithRetry(sql, params) {
  const { runId, fromHourBucket, toHourBucket, nowMs, lockTimeoutMs, maxAttempts } = params;

  let attempt = 0;
  while (attempt < maxAttempts) {
    attempt += 1;

    try {
      const txResults = await sql.transaction((txn) => [
        txn.query(`SET LOCAL lock_timeout = '${lockTimeoutMs}ms'`),
        txn`LOCK TABLE ${txn.unsafe(TREND_COUNT_HOUR_TABLE)} IN ACCESS EXCLUSIVE MODE`,
        txn.query(
          `
          DELETE FROM ${TREND_COUNT_HOUR_TABLE}
          WHERE hour_bucket >= $1
            AND hour_bucket <= $2
          RETURNING 1
          `,
          [fromHourBucket, toHourBucket]
        ),
        txn.query(
          `
          INSERT INTO ${TREND_COUNT_HOUR_TABLE} (hour_bucket, subject_id, count, updated_at)
          SELECT
            hour_bucket,
            subject_id,
            count,
            $2::BIGINT AS updated_at
          FROM ${TREND_COUNT_HOUR_STAGE_TABLE}
          WHERE run_id = $1
          ON CONFLICT (hour_bucket, subject_id) DO UPDATE SET
            count = EXCLUDED.count,
            updated_at = EXCLUDED.updated_at
          RETURNING 1
          `,
          [runId, nowMs]
        ),
      ]);

      const deletedRows = Array.isArray(txResults[2]) ? txResults[2].length : 0;
      const upsertedRows = Array.isArray(txResults[3]) ? txResults[3].length : 0;
      return {
        attempt,
        deletedRows,
        upsertedRows,
      };
    } catch (error) {
      if (!isRetryableApplyError(error) || attempt >= maxAttempts) {
        throw error;
      }

      const backoffMs = Math.min(4000, 200 * 2 ** (attempt - 1));
      await sleep(backoffMs);
    }
  }

  throw new Error("apply hour window exhausted retries");
}

async function main() {
  loadLocalEnvFiles();

  const nowMs = parseArg("now-ms", Date.now());
  const maxAttempts = parseArg("max-attempts", DEFAULT_APPLY_MAX_ATTEMPTS);
  const lockTimeoutMs = parseArg("lock-timeout-ms", DEFAULT_LOCK_TIMEOUT_MS);
  const windowStartMs = getBeijingDayStart(nowMs) - DAY_MS;
  const fromHourBucket = toBeijingHourBucket(windowStartMs);
  const toHourBucket = toBeijingHourBucket(nowMs);
  const runId = `hour-rebuild-${nowMs}-${Math.random().toString(36).slice(2, 10)}`;

  const databaseUrl = process.env.DATABASE_URL || buildDatabaseUrlFromNeonParts();
  if (!databaseUrl) {
    throw new Error("DATABASE_URL / NEON_DATABASE_* is required");
  }

  const sql = neon(databaseUrl);
  await ensureTables(sql);

  const stagedRows = await stageWindowCounts(sql, {
    runId,
    windowStartMs,
    nowMs,
  });

  const applied = await applyStagedWindowWithRetry(sql, {
    runId,
    fromHourBucket,
    toHourBucket,
    nowMs,
    lockTimeoutMs,
    maxAttempts,
  });

  await sql.query(
    `
    DELETE FROM ${TREND_COUNT_HOUR_STAGE_TABLE}
    WHERE run_id = $1
    `,
    [runId]
  );

  console.log(
    JSON.stringify(
      {
        ok: true,
        runId,
        windowStartMs,
        nowMs,
        fromHourBucket,
        toHourBucket,
        stagedRows,
        applyAttempt: applied.attempt,
        deletedRows: applied.deletedRows,
        upsertedRows: applied.upsertedRows,
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
