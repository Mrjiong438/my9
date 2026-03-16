#!/usr/bin/env node

import { neon } from "@neondatabase/serverless";
import { resolve } from "node:path";
import { buildDatabaseUrlFromNeonParts } from "./share-count-utils.mjs";

const SHARE_VIEW_TOTAL_TABLE = "my9_share_view_total_v1";
const SHARES_V2_TABLE = "my9_share_registry_v2";
const SUBJECT_DIM_TABLE = "my9_subject_dim_v1";
const DEFAULT_LIMIT = 100;
const DEFAULT_SITE_URL = "https://my9.shatranj.space";

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

function parseStringArg(name) {
  const prefix = `--${name}=`;
  const arg = process.argv.find((value) => value.startsWith(prefix));
  return arg ? arg.slice(prefix.length).trim() : null;
}

function parseLimitArg() {
  const fallback = DEFAULT_LIMIT;
  const raw = parseStringArg("limit");
  if (!raw) return fallback;
  const parsed = Number(raw);
  if (!Number.isFinite(parsed) || parsed <= 0) return fallback;
  return Math.trunc(parsed);
}

function hasArg(name) {
  return process.argv.includes(`--${name}`);
}

function normalizeSiteUrl(value) {
  const base = (value ?? DEFAULT_SITE_URL).trim();
  return base.replace(/\/+$/, "");
}

function toTimestampMs(value) {
  if (typeof value === "number" && Number.isFinite(value)) return Math.trunc(value);
  if (typeof value === "string" && value.trim()) {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) return Math.trunc(parsed);
  }
  return null;
}

function formatTimestamp(value) {
  const timestamp = toTimestampMs(value);
  if (timestamp === null) return "-";
  return new Date(timestamp).toISOString().replace(".000Z", "Z");
}

function formatPreview(titles) {
  if (titles.length === 0) return "-";
  if (titles.length <= 3) return titles.join(" / ");
  return `${titles.slice(0, 3).join(" / ")} / ...`;
}

function clip(value, maxLength) {
  const text = (value ?? "").trim();
  if (!text) return "-";
  if (text.length <= maxLength) return text;
  return `${text.slice(0, Math.max(1, maxLength - 3))}...`;
}

function printRows(rows) {
  for (const row of rows) {
    const creatorLabel = row.creatorName ? ` @${row.creatorName}` : "";
    console.log(
      `${String(row.rank).padStart(3, " ")}. ${String(row.viewCount).padStart(6, " ")}  ${row.kind.padEnd(
        10,
        " "
      )} ${clip(row.creatorName ?? "-", 16).padEnd(16, " ")} ${row.url}`
    );
    console.log(`     preview: ${row.preview}`);
    console.log(`     updated: ${row.lastAggregatedAtIso}  created: ${row.createdAtIso}${creatorLabel}`);
  }
}

function normalizePayload(payload) {
  return Array.isArray(payload) ? payload : [];
}

async function resolveSubjectNames(sql, rows) {
  const idsByKind = new Map();

  for (const row of rows) {
    for (const item of normalizePayload(row.hot_payload)) {
      const kind = typeof row.kind === "string" ? row.kind.trim() : "";
      const subjectId = typeof item?.sid === "string" ? item.sid.trim() : "";
      if (!kind || !subjectId) continue;
      if (!idsByKind.has(kind)) {
        idsByKind.set(kind, new Set());
      }
      idsByKind.get(kind).add(subjectId);
    }
  }

  const subjectNameMap = new Map();

  for (const [kind, idSet] of idsByKind.entries()) {
    const ids = Array.from(idSet);
    if (ids.length === 0) continue;
    const subjectRows = await sql.query(
      `
      SELECT subject_id, name, localized_name
      FROM ${SUBJECT_DIM_TABLE}
      WHERE kind = $1
        AND subject_id = ANY($2::text[])
      `,
      [kind, ids]
    );

    for (const row of subjectRows) {
      const key = `${kind}:${String(row.subject_id)}`;
      const localized = typeof row.localized_name === "string" ? row.localized_name.trim() : "";
      const name = typeof row.name === "string" ? row.name.trim() : "";
      subjectNameMap.set(key, localized || name || String(row.subject_id));
    }
  }

  return subjectNameMap;
}

async function main() {
  loadLocalEnvFiles();

  const databaseUrl =
    readEnv(
      "NEON_DATABASE_DATABASE_URL_UNPOOLED",
      "NEON_DATABASE_POSTGRES_URL_NON_POOLING",
      "NEON_DATABASE_POSTGRES_URL",
      "NEON_DATABASE_DATABASE_URL"
    ) ?? buildDatabaseUrlFromNeonParts();

  if (!databaseUrl) {
    throw new Error("Missing Neon database connection env.");
  }

  const limit = parseLimitArg();
  const kindFilter = parseStringArg("kind");
  const jsonMode = hasArg("json");
  const siteUrl = normalizeSiteUrl(parseStringArg("site-url") ?? readEnv("SITE_URL") ?? DEFAULT_SITE_URL);
  const sql = neon(databaseUrl);

  const totalExistsRows = await sql.query("SELECT to_regclass($1) IS NOT NULL AS ok", [SHARE_VIEW_TOTAL_TABLE]);
  if (!totalExistsRows[0]?.ok) {
    throw new Error(`${SHARE_VIEW_TOTAL_TABLE} does not exist.`);
  }

  const topRows = await sql.query(
    `
    SELECT
      t.share_id,
      t.kind,
      t.view_count,
      t.last_aggregated_at,
      r.creator_name,
      r.created_at,
      r.hot_payload
    FROM ${SHARE_VIEW_TOTAL_TABLE} t
    LEFT JOIN ${SHARES_V2_TABLE} r ON r.share_id = t.share_id
    WHERE ($1::text IS NULL OR t.kind = $1::text)
    ORDER BY t.view_count DESC, t.share_id ASC
    LIMIT $2
    `,
    [kindFilter, limit]
  );

  const subjectNameMap = await resolveSubjectNames(sql, topRows);

  const outputRows = topRows.map((row, index) => {
    const previewTitles = normalizePayload(row.hot_payload)
      .map((item) => {
        const subjectId = typeof item?.sid === "string" ? item.sid.trim() : "";
        if (!subjectId) return null;
        return subjectNameMap.get(`${row.kind}:${subjectId}`) ?? subjectId;
      })
      .filter(Boolean);

    return {
      rank: index + 1,
      shareId: row.share_id,
      kind: row.kind,
      creatorName: row.creator_name ?? null,
      viewCount: Number(row.view_count),
      lastAggregatedAtIso: formatTimestamp(row.last_aggregated_at),
      createdAtIso: formatTimestamp(row.created_at),
      url: `${siteUrl}/${row.kind}/s/${row.share_id}`,
      preview: formatPreview(previewTitles),
    };
  });

  if (jsonMode) {
    console.log(JSON.stringify(outputRows, null, 2));
    return;
  }

  console.log(
    `[share-view-top] total=${outputRows.length} limit=${limit}${kindFilter ? ` kind=${kindFilter}` : ""} site=${siteUrl}`
  );
  printRows(outputRows);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
