import { normalizeShareId } from "@/lib/share/id";
import {
  getShareViewRollupCheckpoint,
  setShareViewRollupCheckpoint,
  upsertShareViewTotalCounts,
} from "@/lib/share/storage";
import { parseSubjectKind, type SubjectKind } from "@/lib/subject-kind";

const ANALYTICS_SQL_API_BASE = "https://api.cloudflare.com/client/v4";
const BEIJING_TZ_OFFSET_MS = 8 * 60 * 60 * 1000;
const DAY_MS = 24 * 60 * 60 * 1000;
const DATASET_NAME_PATTERN = /^[A-Za-z0-9_]+$/;
const SHARE_PAGE_PATH_PATTERN = /^\/([^/]+)\/s\/([^/]+)$/;

type AnalyticsEngineWriter = {
  writeDataPoint: (event: {
    blobs?: string[];
    doubles?: number[];
    indexes?: string[];
  }) => void;
};

type WorkerEnvLike = Record<string, unknown> | undefined;

type ShareViewLogTarget = {
  shareId: string;
  kind: SubjectKind;
  host: string;
};

type ShareViewRollupRow = {
  shareId: string;
  kind: SubjectKind;
  viewCount: number;
};

export type ShareViewRollupResult = {
  ok: true;
  skipped: boolean;
  reason?: string;
  dataset?: string;
  rowsFetched: number;
  rowsWritten: number;
  closedThroughDayKey: number | null;
};

function readString(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  return trimmed ? trimmed : null;
}

function readEnvString(env: WorkerEnvLike, key: string): string | null {
  const fromEnv = readString(env?.[key]);
  if (fromEnv) return fromEnv;
  return readString(process.env[key]);
}

function toBeijingDayKey(timestampMs: number): number {
  const date = new Date(timestampMs + BEIJING_TZ_OFFSET_MS);
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  const day = String(date.getUTCDate()).padStart(2, "0");
  return Number(`${year}${month}${day}`);
}

function getBeijingDayStart(timestampMs: number): number {
  return Math.floor((timestampMs + BEIJING_TZ_OFFSET_MS) / DAY_MS) * DAY_MS - BEIJING_TZ_OFFSET_MS;
}

function toSqlUtcDateTime(timestampMs: number): string {
  return new Date(timestampMs).toISOString().slice(0, 19).replace("T", " ");
}

function parseJsonEachRow<T>(input: string): T[] {
  const text = input.trim();
  if (!text) return [];
  return text
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => JSON.parse(line) as T);
}

function shouldSkipPrefetch(request: Request): boolean {
  if (request.headers.get("next-router-prefetch")) {
    return true;
  }

  for (const header of ["purpose", "sec-purpose", "x-moz"]) {
    const value = request.headers.get(header)?.toLowerCase();
    if (value?.includes("prefetch")) {
      return true;
    }
  }

  return false;
}

function resolveShareViewLogTarget(request: Request): ShareViewLogTarget | null {
  if (request.method !== "GET" || shouldSkipPrefetch(request)) {
    return null;
  }

  const url = new URL(request.url);
  const match = SHARE_PAGE_PATH_PATTERN.exec(url.pathname);
  if (!match) {
    return null;
  }

  const kind = parseSubjectKind(match[1]);
  const shareId = normalizeShareId(match[2]);
  if (!kind || !shareId) {
    return null;
  }

  const destination = request.headers.get("sec-fetch-dest")?.toLowerCase();
  if (destination && destination !== "document") {
    return null;
  }

  const mode = request.headers.get("sec-fetch-mode")?.toLowerCase();
  const accept = request.headers.get("accept")?.toLowerCase() ?? "";
  if (mode && mode !== "navigate" && !accept.includes("text/html")) {
    return null;
  }

  return {
    shareId,
    kind,
    host: url.hostname,
  };
}

export function trackShareViewRequest(
  request: Request,
  analyticsDataset: AnalyticsEngineWriter | null | undefined
): boolean {
  const target = resolveShareViewLogTarget(request);
  if (!target || !analyticsDataset || typeof analyticsDataset.writeDataPoint !== "function") {
    return false;
  }

  try {
    analyticsDataset.writeDataPoint({
      indexes: [target.shareId],
      blobs: [target.kind, target.host],
      doubles: [1],
    });
    return true;
  } catch (error) {
    console.error("[share-view-track] failed", error);
    return false;
  }
}

function resolveRollupConfig(env?: WorkerEnvLike) {
  return {
    accountId:
      readEnvString(env, "MY9_ANALYTICS_ACCOUNT_ID") ?? readEnvString(env, "CLOUDFLARE_ACCOUNT_ID"),
    apiToken: readEnvString(env, "MY9_ANALYTICS_API_TOKEN") ?? readEnvString(env, "CLOUDFLARE_API_TOKEN"),
    dataset: readEnvString(env, "MY9_SHARE_VIEW_ANALYTICS_DATASET"),
  };
}

function assertDatasetName(dataset: string): string {
  if (!DATASET_NAME_PATTERN.test(dataset)) {
    throw new Error(`invalid analytics dataset name: ${dataset}`);
  }
  return dataset;
}

async function queryAnalyticsSql(accountId: string, apiToken: string, query: string): Promise<string> {
  const response = await fetch(`${ANALYTICS_SQL_API_BASE}/accounts/${accountId}/analytics_engine/sql`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${apiToken}`,
    },
    body: query,
  });

  const text = await response.text();
  if (!response.ok) {
    throw new Error(`analytics query failed: ${response.status} ${response.statusText} ${text}`.trim());
  }

  return text;
}

async function queryShareViewTotals(
  accountId: string,
  apiToken: string,
  dataset: string,
  endMs: number,
  startMs?: number | null
): Promise<ShareViewRollupRow[]> {
  if (typeof startMs === "number" && Number.isFinite(startMs) && startMs >= endMs) {
    return [];
  }

  const timeFilter =
    typeof startMs === "number" && Number.isFinite(startMs)
      ? `timestamp >= toDateTime('${toSqlUtcDateTime(startMs)}') AND timestamp < toDateTime('${toSqlUtcDateTime(endMs)}')`
      : `timestamp < toDateTime('${toSqlUtcDateTime(endMs)}')`;
  const query = `
SELECT
  index1 AS share_id,
  blob1 AS kind,
  SUM(_sample_interval * double1) AS view_count
FROM ${assertDatasetName(dataset)}
WHERE ${timeFilter}
GROUP BY index1, blob1
FORMAT JSONEachRow
`.trim();

  const rows = parseJsonEachRow<Array<Record<string, unknown>>[number]>(
    await queryAnalyticsSql(accountId, apiToken, query)
  );

  return rows
    .map((row) => {
      const shareId = normalizeShareId(readString(row.share_id));
      const kind = parseSubjectKind(readString(row.kind));
      const viewCount = Number(row.view_count);

      if (!shareId || !kind || !Number.isFinite(viewCount) || viewCount <= 0) {
        return null;
      }

      return {
        shareId,
        kind,
        viewCount: Math.trunc(viewCount),
      } satisfies ShareViewRollupRow;
    })
    .filter((row): row is ShareViewRollupRow => Boolean(row));
}

export async function runShareViewRollup(options?: {
  env?: WorkerEnvLike;
  nowMs?: number;
  logLabel?: string;
}): Promise<ShareViewRollupResult> {
  const config = resolveRollupConfig(options?.env);
  const dataset = config.dataset ? assertDatasetName(config.dataset) : null;

  if (!config.accountId || !config.apiToken || !dataset) {
    const result: ShareViewRollupResult = {
      ok: true,
      skipped: true,
      reason: "missing analytics config",
      rowsFetched: 0,
      rowsWritten: 0,
      closedThroughDayKey: null,
    };
    if (options?.logLabel) {
      console.log(`${options.logLabel} ${JSON.stringify(result)}`);
    }
    return result;
  }

  const nowMs = options?.nowMs ?? Date.now();
  const currentDayStart = getBeijingDayStart(nowMs);
  const lastCheckpointMs = await getShareViewRollupCheckpoint();
  const startMs =
    typeof lastCheckpointMs === "number" && Number.isFinite(lastCheckpointMs)
      ? Math.min(currentDayStart, getBeijingDayStart(lastCheckpointMs))
      : null;
  const rows = await queryShareViewTotals(config.accountId, config.apiToken, dataset, currentDayStart, startMs);
  const rowsFetched = rows.length;
  const rowsWritten = await upsertShareViewTotalCounts(
    rows.map((row) => ({
      shareId: row.shareId,
      kind: row.kind,
      viewCount: row.viewCount,
    })),
    {
      lastAggregatedAt: nowMs,
      mode: startMs === null ? "replace" : "increment",
    }
  );
  await setShareViewRollupCheckpoint(currentDayStart);

  const result: ShareViewRollupResult = {
    ok: true,
    skipped: false,
    dataset,
    rowsFetched,
    rowsWritten,
    closedThroughDayKey: toBeijingDayKey(currentDayStart - 1),
  };

  if (options?.logLabel) {
    console.log(`${options.logLabel} ${JSON.stringify(result)}`);
  }

  return result;
}
