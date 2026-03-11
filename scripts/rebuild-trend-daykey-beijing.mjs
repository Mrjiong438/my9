#!/usr/bin/env node

import { neon } from "@neondatabase/serverless";
import { GetObjectCommand, S3Client } from "@aws-sdk/client-s3";
import { resolve } from "node:path";
import { gunzipSync } from "node:zlib";

const SHARES_V2_TABLE = "my9_share_registry_v2";
const TREND_COUNT_ALL_TABLE = "my9_trend_subject_all_v2";
const TREND_COUNT_DAY_TABLE = "my9_trend_subject_day_v2";
const TRENDS_CACHE_TABLE = "my9_trends_cache_v1";
const BEIJING_TZ_OFFSET_MS = 8 * 60 * 60 * 1000;

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

function hasArg(name) {
  return process.argv.includes(`--${name}`);
}

function toBeijingDayKey(timestampMs) {
  const date = new Date(timestampMs + BEIJING_TZ_OFFSET_MS);
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  const day = String(date.getUTCDate()).padStart(2, "0");
  return Number(`${year}${month}${day}`);
}

function extractSubjectIds(payload) {
  if (!Array.isArray(payload) || payload.length !== 9) {
    return [];
  }

  const subjectIds = [];
  for (const slot of payload) {
    if (!slot || typeof slot !== "object") continue;
    const sid = typeof slot.sid === "string" ? slot.sid.trim() : "";
    if (sid) subjectIds.push(sid);
  }
  return subjectIds;
}

async function bodyToBuffer(body) {
  if (!body) return Buffer.alloc(0);
  if (Buffer.isBuffer(body)) return body;
  if (typeof body.transformToByteArray === "function") {
    return Buffer.from(await body.transformToByteArray());
  }
  if (typeof body.arrayBuffer === "function") {
    return Buffer.from(await body.arrayBuffer());
  }

  const chunks = [];
  for await (const chunk of body) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }
  return Buffer.concat(chunks);
}

function createR2Client() {
  const endpoint = readEnv("R2_ENDPOINT");
  const bucket = readEnv("R2_BUCKET");
  const accessKeyId = readEnv("R2_ACCESS_KEY_ID");
  const secretAccessKey = readEnv("R2_SECRET_ACCESS_KEY");
  const region = readEnv("R2_REGION") ?? "auto";

  if (!endpoint || !bucket || !accessKeyId || !secretAccessKey) {
    return null;
  }

  return {
    bucket,
    s3: new S3Client({
      endpoint,
      region,
      forcePathStyle: true,
      credentials: {
        accessKeyId,
        secretAccessKey,
      },
    }),
  };
}

async function readColdPayload(s3, bucket, key) {
  const response = await s3.send(
    new GetObjectCommand({
      Bucket: bucket,
      Key: key,
    })
  );
  const compressed = await bodyToBuffer(response.Body);
  const jsonText = gunzipSync(compressed).toString("utf8");
  return JSON.parse(jsonText);
}

async function main() {
  loadLocalEnvFiles();

  const force = hasArg("force");
  const databaseUrl = process.env.DATABASE_URL || buildDatabaseUrlFromNeonParts();
  if (!databaseUrl) {
    throw new Error("DATABASE_URL / NEON_DATABASE_* is required");
  }

  const sql = neon(databaseUrl);
  const now = Date.now();

  const nullPayloadRows = await sql.query(
    `
    SELECT share_id, storage_tier, cold_object_key, created_at
    FROM ${SHARES_V2_TABLE}
    WHERE hot_payload IS NULL
    `
  );

  const recoverableRows = nullPayloadRows.filter((row) => typeof row.cold_object_key === "string" && row.cold_object_key);
  const unrecoverableRows = nullPayloadRows.filter((row) => !(typeof row.cold_object_key === "string" && row.cold_object_key));

  if (unrecoverableRows.length > 0 && !force) {
    throw new Error(
      `found ${unrecoverableRows.length} null-payload rows without cold_object_key. ` +
        "Run with --force only if you intentionally accept data loss for those rows."
    );
  }

  let r2Client = null;
  if (recoverableRows.length > 0) {
    r2Client = createR2Client();
    if (!r2Client && !force) {
      throw new Error(
        `found ${recoverableRows.length} cold rows but R2 env is missing. ` +
          "Set R2_* env vars or run with --force to skip cold rows."
      );
    }
  }

  const coldAllCountMap = new Map();
  const coldDayCountMap = new Map();
  let recoveredColdRows = 0;
  let skippedColdRows = 0;

  if (r2Client && recoverableRows.length > 0) {
    for (const row of recoverableRows) {
      try {
        const payload = await readColdPayload(r2Client.s3, r2Client.bucket, row.cold_object_key);
        const subjectIds = extractSubjectIds(payload);
        const createdAt = Number(row.created_at || 0);
        const dayKey = toBeijingDayKey(createdAt);

        for (const subjectId of subjectIds) {
          coldAllCountMap.set(subjectId, (coldAllCountMap.get(subjectId) ?? 0) + 1);
          const dayKeySubjectId = `${dayKey}|${subjectId}`;
          coldDayCountMap.set(dayKeySubjectId, (coldDayCountMap.get(dayKeySubjectId) ?? 0) + 1);
        }
        recoveredColdRows += 1;
      } catch (error) {
        skippedColdRows += 1;
        if (!force) {
          throw new Error(
            `failed to read cold payload for ${row.share_id}: ${
              error instanceof Error ? error.message : String(error)
            }`
          );
        }
      }
    }
  } else if (recoverableRows.length > 0) {
    skippedColdRows = recoverableRows.length;
  }

  const coldAllRows = Array.from(coldAllCountMap.entries()).map(([subject_id, count]) => ({
    subject_id,
    count,
  }));
  const coldDayRows = Array.from(coldDayCountMap.entries()).map(([key, count]) => {
    const separatorIndex = key.indexOf("|");
    return {
      day_key: Number(key.slice(0, separatorIndex)),
      subject_id: key.slice(separatorIndex + 1),
      count,
    };
  });

  await sql.transaction((txn) => [
    txn`TRUNCATE TABLE ${txn.unsafe(TREND_COUNT_ALL_TABLE)}`,
    txn`TRUNCATE TABLE ${txn.unsafe(TREND_COUNT_DAY_TABLE)}`,
    txn`TRUNCATE TABLE ${txn.unsafe(TRENDS_CACHE_TABLE)}`,
    txn`
      WITH share_slots AS (
        SELECT
          BTRIM(slot->>'sid') AS subject_id
        FROM ${txn.unsafe(SHARES_V2_TABLE)} s
        CROSS JOIN LATERAL jsonb_array_elements(
          CASE
            WHEN s.hot_payload IS NULL THEN '[]'::jsonb
            WHEN jsonb_typeof(s.hot_payload) = 'array' THEN s.hot_payload
            ELSE '[]'::jsonb
          END
        ) AS slot
        WHERE s.hot_payload IS NOT NULL
      )
      INSERT INTO ${txn.unsafe(TREND_COUNT_ALL_TABLE)} (subject_id, count, updated_at)
      SELECT
        subject_id,
        COUNT(*)::BIGINT AS count,
        ${now}::BIGINT AS updated_at
      FROM share_slots
      WHERE subject_id <> ''
      GROUP BY subject_id
      ON CONFLICT (subject_id) DO UPDATE SET
        count = EXCLUDED.count,
        updated_at = EXCLUDED.updated_at
    `,
    txn`
      WITH share_slots AS (
        SELECT
          TO_CHAR(
            timezone('Asia/Shanghai', to_timestamp(s.created_at / 1000.0)),
            'YYYYMMDD'
          )::INT AS day_key,
          BTRIM(slot->>'sid') AS subject_id
        FROM ${txn.unsafe(SHARES_V2_TABLE)} s
        CROSS JOIN LATERAL jsonb_array_elements(
          CASE
            WHEN s.hot_payload IS NULL THEN '[]'::jsonb
            WHEN jsonb_typeof(s.hot_payload) = 'array' THEN s.hot_payload
            ELSE '[]'::jsonb
          END
        ) AS slot
        WHERE s.hot_payload IS NOT NULL
      )
      INSERT INTO ${txn.unsafe(TREND_COUNT_DAY_TABLE)} (day_key, subject_id, count, updated_at)
      SELECT
        day_key,
        subject_id,
        COUNT(*)::BIGINT AS count,
        ${now}::BIGINT AS updated_at
      FROM share_slots
      WHERE subject_id <> ''
      GROUP BY day_key, subject_id
      ON CONFLICT (day_key, subject_id) DO UPDATE SET
        count = EXCLUDED.count,
        updated_at = EXCLUDED.updated_at
    `,
    txn`
      WITH cold_rows AS (
        SELECT r.subject_id, r.count
        FROM jsonb_to_recordset(COALESCE(${JSON.stringify(coldAllRows)}::jsonb, '[]'::jsonb)) AS r(subject_id text, count bigint)
      )
      INSERT INTO ${txn.unsafe(TREND_COUNT_ALL_TABLE)} (subject_id, count, updated_at)
      SELECT subject_id, count, ${now}::bigint
      FROM cold_rows
      ON CONFLICT (subject_id) DO UPDATE SET
        count = ${txn.unsafe(TREND_COUNT_ALL_TABLE)}.count + EXCLUDED.count,
        updated_at = EXCLUDED.updated_at
    `,
    txn`
      WITH cold_rows AS (
        SELECT r.day_key, r.subject_id, r.count
        FROM jsonb_to_recordset(COALESCE(${JSON.stringify(coldDayRows)}::jsonb, '[]'::jsonb)) AS r(day_key int, subject_id text, count bigint)
      )
      INSERT INTO ${txn.unsafe(TREND_COUNT_DAY_TABLE)} (day_key, subject_id, count, updated_at)
      SELECT day_key, subject_id, count, ${now}::bigint
      FROM cold_rows
      ON CONFLICT (day_key, subject_id) DO UPDATE SET
        count = ${txn.unsafe(TREND_COUNT_DAY_TABLE)}.count + EXCLUDED.count,
        updated_at = EXCLUDED.updated_at
    `,
  ]);

  const allRows = await sql.query(`SELECT COUNT(*)::BIGINT AS total FROM ${TREND_COUNT_ALL_TABLE}`);
  const dayRows = await sql.query(`SELECT COUNT(*)::BIGINT AS total FROM ${TREND_COUNT_DAY_TABLE}`);

  console.log(
    JSON.stringify(
      {
        ok: true,
        force,
        nullPayloadRows: nullPayloadRows.length,
        recoverableRows: recoverableRows.length,
        unrecoverableRows: unrecoverableRows.length,
        recoveredColdRows,
        skippedColdRows,
        allRows: Number(allRows[0]?.total || 0),
        dayRows: Number(dayRows[0]?.total || 0),
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
