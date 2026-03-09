#!/usr/bin/env node

import { readFileSync } from "node:fs";
import { basename } from "node:path";

const WINDOW_MINUTES = 15;
const VALID_KINDS = new Set(["game", "anime", "manga", "lightnovel", "work"]);

const ALERT_THRESHOLDS = {
  search_total: 1800,
  share_rsc_html_pair_rate: 0.35,
  invalid_kind_404: 10,
  api_share_post_rate: 6,
};

function parseCsvLine(line) {
  const cells = [];
  let current = "";
  let inQuotes = false;

  for (let index = 0; index < line.length; index += 1) {
    const char = line[index];
    if (inQuotes) {
      if (char === "\"") {
        const next = line[index + 1];
        if (next === "\"") {
          current += "\"";
          index += 1;
        } else {
          inQuotes = false;
        }
      } else {
        current += char;
      }
      continue;
    }

    if (char === "\"") {
      inQuotes = true;
      continue;
    }

    if (char === ",") {
      cells.push(current);
      current = "";
      continue;
    }

    current += char;
  }

  cells.push(current);
  return cells;
}

function parseCsv(content) {
  const lines = content
    .split(/\r?\n/)
    .map((line) => line.trimEnd())
    .filter((line) => line.length > 0);

  if (lines.length === 0) return [];
  const header = parseCsvLine(lines[0]);
  const rows = [];

  for (const line of lines.slice(1)) {
    const values = parseCsvLine(line);
    const row = {};
    for (let index = 0; index < header.length; index += 1) {
      row[header[index]] = values[index] || "";
    }
    rows.push(row);
  }

  return rows;
}

function parseUtcSecond(value) {
  if (!value) return null;
  const isoText = value.replace(" ", "T");
  const parsed = Date.parse(`${isoText}Z`);
  if (Number.isNaN(parsed)) return null;
  return parsed;
}

function normalizePath(rawPath) {
  if (!rawPath) return "/";
  const schemeSplit = rawPath.split("://");
  const withoutScheme = schemeSplit.length > 1 ? schemeSplit.slice(1).join("://") : schemeSplit[0];
  const hostRemoved = withoutScheme.replace(/^[^/]+/, "");
  if (!hostRemoved) return "/";
  return hostRemoved.startsWith("/") ? hostRemoved : `/${hostRemoved}`;
}

function floorToWindow(tsMs, windowMinutes) {
  const d = new Date(tsMs);
  d.setUTCSeconds(0, 0);
  const minute = d.getUTCMinutes();
  d.setUTCMinutes(minute - (minute % windowMinutes));
  return d.getTime();
}

function formatWindowStart(tsMs) {
  return new Date(tsMs).toISOString().replace(".000Z", "Z");
}

function extractShareId(pathname) {
  const match = pathname.match(/^\/[^/]+\/s\/([^/?]+)/);
  return match ? match[1] : null;
}

function toBucketKey(tsMs) {
  return String(floorToWindow(tsMs, WINDOW_MINUTES));
}

function computeWindows(rows) {
  const windowMap = new Map();

  for (const row of rows) {
    const tsMs = parseUtcSecond(row.TimeUTC);
    if (tsMs === null) continue;

    const key = toBucketKey(tsMs);
    if (!windowMap.has(key)) {
      windowMap.set(key, {
        windowStartMs: Number(key),
        totalRequests: 0,
        searchTotal: 0,
        invalidKind404: 0,
        apiSharePostCount: 0,
        rscEvents: [],
        htmlEventsByShareId: new Map(),
      });
    }

    const bucket = windowMap.get(key);
    const fn = row.function || "";
    const method = row.requestMethod || "";
    const status = row.responseStatusCode || "";
    const path = normalizePath(row.requestPath || "");
    const tsSec = Math.floor(tsMs / 1000);

    bucket.totalRequests += 1;

    if (fn === "/api/subjects/search") {
      bucket.searchTotal += 1;
    }

    if (fn === "/api/share" && method === "POST") {
      bucket.apiSharePostCount += 1;
    }

    if (fn === "/[kind]" && status === "404") {
      const segment = path.replace(/^\/+/, "").split("/")[0] || "";
      if (!VALID_KINDS.has(segment)) {
        bucket.invalidKind404 += 1;
      }
    }

    if (fn === "/[kind]/s/[shareId]") {
      const shareId = extractShareId(path);
      if (shareId) {
        const list = bucket.htmlEventsByShareId.get(shareId) || [];
        list.push(tsSec);
        bucket.htmlEventsByShareId.set(shareId, list);
      }
    }

    if (fn === "/[kind]/s/[shareId].rsc") {
      const shareId = extractShareId(path);
      if (shareId) {
        bucket.rscEvents.push({ shareId, tsSec });
      }
    }
  }

  const outputs = [];
  for (const bucket of windowMap.values()) {
    let matchedPairCount = 0;
    for (const rsc of bucket.rscEvents) {
      const htmlTimes = bucket.htmlEventsByShareId.get(rsc.shareId) || [];
      if (htmlTimes.some((htmlSec) => Math.abs(htmlSec - rsc.tsSec) <= 2)) {
        matchedPairCount += 1;
      }
    }

    const sharePairRate =
      bucket.rscEvents.length === 0 ? 0 : matchedPairCount / bucket.rscEvents.length;
    const apiSharePostRate = bucket.apiSharePostCount / WINDOW_MINUTES;

    outputs.push({
      window_start: formatWindowStart(bucket.windowStartMs),
      total_requests: bucket.totalRequests,
      search_total: bucket.searchTotal,
      share_rsc_html_pair_rate: sharePairRate,
      invalid_kind_404: bucket.invalidKind404,
      api_share_post_rate: apiSharePostRate,
    });
  }

  return outputs.sort((a, b) => a.window_start.localeCompare(b.window_start));
}

function checkAlerts(row) {
  return {
    search_total: row.search_total > ALERT_THRESHOLDS.search_total,
    share_rsc_html_pair_rate:
      row.share_rsc_html_pair_rate > ALERT_THRESHOLDS.share_rsc_html_pair_rate,
    invalid_kind_404: row.invalid_kind_404 > ALERT_THRESHOLDS.invalid_kind_404,
    api_share_post_rate: row.api_share_post_rate > ALERT_THRESHOLDS.api_share_post_rate,
  };
}

function formatRate(value) {
  return `${(value * 100).toFixed(1)}%`;
}

function main() {
  const inputPath = process.argv[2];
  if (!inputPath) {
    console.error("Usage: node scripts/analyze-edge-log.mjs <csv-file>");
    process.exit(1);
  }

  const content = readFileSync(inputPath, "utf8");
  const rows = parseCsv(content);
  const windows = computeWindows(rows);

  console.log(`Edge log: ${basename(inputPath)}`);
  console.log(`Rows: ${rows.length}`);
  console.log(`Window: ${WINDOW_MINUTES} minutes`);
  console.log("");

  for (const row of windows) {
    const alerts = checkAlerts(row);
    const alertKeys = Object.entries(alerts)
      .filter(([, active]) => active)
      .map(([key]) => key);

    console.log(
      [
        row.window_start,
        `total=${row.total_requests}`,
        `search_total=${row.search_total}`,
        `share_pair_rate=${formatRate(row.share_rsc_html_pair_rate)}`,
        `invalid_kind_404=${row.invalid_kind_404}`,
        `api_share_post_rate=${row.api_share_post_rate.toFixed(2)}/min`,
        alertKeys.length > 0 ? `ALERT:${alertKeys.join("|")}` : "OK",
      ].join("  ")
    );
  }

  console.log("");
  console.log("Alert thresholds:");
  console.log(
    `search_total>${ALERT_THRESHOLDS.search_total}, share_rsc_html_pair_rate>${ALERT_THRESHOLDS.share_rsc_html_pair_rate}, invalid_kind_404>${ALERT_THRESHOLDS.invalid_kind_404}, api_share_post_rate>${ALERT_THRESHOLDS.api_share_post_rate}/min`
  );
}

main();
