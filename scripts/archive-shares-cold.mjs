#!/usr/bin/env node

import { neon } from "@neondatabase/serverless";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { resolve } from "node:path";
import { gzipSync } from "node:zlib";

const SHARES_V2_TABLE = "my9_share_registry_v2";
const TREND_COUNT_DAY_TABLE = "my9_trend_subject_day_v2";

function loadLocalEnvFiles() {
  const candidates = [".env.local", ".env"];
  for (const file of candidates) {
    try {
      process.loadEnvFile(resolve(process.cwd(), file));
    } catch {
      // ignore missing env file
    }
  }
}

loadLocalEnvFiles();

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

const DAY_MS = 24 * 60 * 60 * 1000;
const BEIJING_TZ_OFFSET_MS = 8 * 60 * 60 * 1000;

function toBeijingDayKey(timestampMs) {
  const date = new Date(timestampMs + BEIJING_TZ_OFFSET_MS);
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  const day = String(date.getUTCDate()).padStart(2, "0");
  return Number(`${year}${month}${day}`);
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

function isValidPayload(value) {
  return Array.isArray(value) && value.length === 9;
}

function buildColdObjectKey(shareId) {
  return `shares/v1/${shareId}.json.gz`;
}

async function main() {
  const olderThanDays = parseArg("older-than-days", 30);
  const batchSize = parseArg("batch-size", 500);
  const cleanupTrendDays = parseArg("cleanup-trend-days", 190);

  const databaseUrl = process.env.DATABASE_URL || buildDatabaseUrlFromNeonParts();
  if (!databaseUrl) {
    throw new Error("DATABASE_URL / NEON_DATABASE_* is required");
  }

  const r2Endpoint = readEnv("R2_ENDPOINT");
  const r2Bucket = readEnv("R2_BUCKET");
  const r2AccessKeyId = readEnv("R2_ACCESS_KEY_ID");
  const r2SecretAccessKey = readEnv("R2_SECRET_ACCESS_KEY");
  const r2Region = readEnv("R2_REGION") ?? "auto";

  if (!r2Endpoint || !r2Bucket || !r2AccessKeyId || !r2SecretAccessKey) {
    throw new Error("R2 config missing: R2_ENDPOINT/R2_BUCKET/R2_ACCESS_KEY_ID/R2_SECRET_ACCESS_KEY");
  }

  const sql = neon(databaseUrl);
  const s3 = new S3Client({
    endpoint: r2Endpoint,
    region: r2Region,
    forcePathStyle: true,
    credentials: {
      accessKeyId: r2AccessKeyId,
      secretAccessKey: r2SecretAccessKey,
    },
  });

  const cutoff = Date.now() - olderThanDays * 24 * 60 * 60 * 1000;
  const rows = await sql.query(
    `
    SELECT share_id, hot_payload, created_at
    FROM ${SHARES_V2_TABLE}
    WHERE storage_tier = 'hot'
      AND hot_payload IS NOT NULL
      AND created_at < $1
    ORDER BY created_at ASC
    LIMIT $2
    `,
    [cutoff, batchSize]
  );

  let archived = 0;
  let skipped = 0;

  for (const row of rows) {
    if (!isValidPayload(row.hot_payload)) {
      skipped += 1;
      continue;
    }

    const objectKey = buildColdObjectKey(row.share_id);
    const body = gzipSync(Buffer.from(JSON.stringify(row.hot_payload), "utf8"));

    try {
      await s3.send(
        new PutObjectCommand({
          Bucket: r2Bucket,
          Key: objectKey,
          Body: body,
          ContentType: "application/json",
          ContentEncoding: "gzip",
        })
      );

      await sql.query(
        `
        UPDATE ${SHARES_V2_TABLE}
        SET
          storage_tier = 'cold',
          cold_object_key = $2,
          hot_payload = NULL,
          updated_at = $3
        WHERE share_id = $1
        `,
        [row.share_id, objectKey, Date.now()]
      );
      archived += 1;
    } catch (error) {
      skipped += 1;
      console.error(`[archive] upload/update failed for ${row.share_id}:`, error instanceof Error ? error.message : error);
    }
  }

  const cleanupBeforeKey = toBeijingDayKey(Date.now() - cleanupTrendDays * DAY_MS);
  const deleted = await sql.query(
    `
    DELETE FROM ${TREND_COUNT_DAY_TABLE}
    WHERE day_key < $1
    RETURNING 1
    `,
    [cleanupBeforeKey]
  );

  console.log(
    JSON.stringify(
      {
        ok: true,
        processed: rows.length,
        archived,
        skipped,
        cleanedTrendRows: deleted.length,
        olderThanDays,
        batchSize,
        cleanupTrendDays,
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
