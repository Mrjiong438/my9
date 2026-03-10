import {
  getAggregatedTrendResponse,
  getTrendSampleSummaryCache,
  listSharesByPeriod,
  setTrendSampleSummaryCache,
  getTrendsCache,
  setTrendsCache,
} from "@/lib/share/storage";
import { buildTrendResponse } from "@/lib/share/trends";
import { TrendPeriod, TrendResponse, TrendView, TrendYearPage } from "@/lib/share/types";
import { DEFAULT_SUBJECT_KIND, SubjectKind, parseSubjectKind } from "@/lib/subject-kind";

export const VALID_TREND_PERIODS: TrendPeriod[] = ["today", "24h", "7d", "30d", "90d", "180d", "all"];
export const VALID_TREND_VIEWS: TrendView[] = ["overall", "genre", "decade", "year"];
export const DEFAULT_TREND_PERIOD: TrendPeriod = "24h";
export const DEFAULT_TREND_VIEW: TrendView = "overall";
export const DEFAULT_TREND_KIND: SubjectKind = DEFAULT_SUBJECT_KIND;
export const DEFAULT_TREND_OVERALL_PAGE = 1;
export const DEFAULT_TREND_YEAR_PAGE: TrendYearPage = "recent";
const MAX_TREND_OVERALL_PAGE = 5;

const TRENDS_STORE_CACHE_TTL_SECONDS = 3600;

function applySampleSummary(
  response: TrendResponse,
  summary: { sampleCount: number; range: { from: number | null; to: number | null } } | null
): TrendResponse {
  if (!summary) {
    return response;
  }
  return {
    ...response,
    sampleCount: summary.sampleCount,
    range: summary.range,
  };
}

function toSampleSummary(response: TrendResponse): {
  sampleCount: number;
  range: { from: number | null; to: number | null };
} {
  return {
    sampleCount: response.sampleCount,
    range: response.range,
  };
}

export function parseTrendPeriod(value: string | null | undefined): TrendPeriod {
  if (value && VALID_TREND_PERIODS.includes(value as TrendPeriod)) {
    return value as TrendPeriod;
  }
  return DEFAULT_TREND_PERIOD;
}

export function parseTrendView(value: string | null | undefined): TrendView {
  if (value && VALID_TREND_VIEWS.includes(value as TrendView)) {
    return value as TrendView;
  }
  return DEFAULT_TREND_VIEW;
}

export function parseTrendKind(value: string | null | undefined): SubjectKind {
  return parseSubjectKind(value) ?? DEFAULT_TREND_KIND;
}

export function parseTrendOverallPage(value: string | null | undefined): number {
  const parsed = Number(value);
  if (!Number.isInteger(parsed)) {
    return DEFAULT_TREND_OVERALL_PAGE;
  }
  if (parsed < 1 || parsed > MAX_TREND_OVERALL_PAGE) {
    return DEFAULT_TREND_OVERALL_PAGE;
  }
  return parsed;
}

export function parseTrendYearPage(value: string | null | undefined): TrendYearPage {
  return value === "legacy" ? "legacy" : DEFAULT_TREND_YEAR_PAGE;
}

function applyFallbackPageFilter(
  response: TrendResponse,
  params: { view: TrendView; overallPage: number; yearPage: TrendYearPage }
): TrendResponse {
  const { view, overallPage, yearPage } = params;
  if (view === "overall") {
    const pageSize = 20;
    const offset = Math.max(0, (overallPage - 1) * pageSize);
    return {
      ...response,
      items: response.items.slice(offset, offset + pageSize),
    };
  }

  if (view === "year") {
    const filtered = response.items.filter((bucket) => {
      const year = Number(bucket.key);
      if (!Number.isFinite(year)) {
        return false;
      }
      return yearPage === "recent" ? year >= 2010 : year <= 2009;
    });
    return {
      ...response,
      items: filtered,
    };
  }

  return response;
}

export async function resolveTrendResponse(params: {
  period: TrendPeriod;
  view: TrendView;
  kind: SubjectKind;
  overallPage: number;
  yearPage: TrendYearPage;
}): Promise<TrendResponse> {
  const { period, view, kind, overallPage, yearPage } = params;
  let sampleSummary = await getTrendSampleSummaryCache(period, kind);

  const cached = await getTrendsCache(period, view, kind, overallPage, yearPage);
  if (cached) {
    if (!sampleSummary) {
      sampleSummary = toSampleSummary(cached);
      await setTrendSampleSummaryCache(period, kind, sampleSummary, TRENDS_STORE_CACHE_TTL_SECONDS);
    }
    const refreshed = applySampleSummary(cached, sampleSummary);
    return refreshed.sampleCount < 30
      ? {
          ...refreshed,
          items: [],
        }
      : refreshed;
  }

  let response: TrendResponse;
  try {
    const aggregated = await getAggregatedTrendResponse({
      period,
      view,
      kind,
      overallPage,
      yearPage,
    });
    if (aggregated && aggregated.sampleCount > 0) {
      response = aggregated;
    } else {
      const shares = (await listSharesByPeriod(period)).filter((item) => item.kind === kind);
      response = applyFallbackPageFilter(
        buildTrendResponse({
          period,
          view,
          kind,
          shares,
        }),
        { view, overallPage, yearPage }
      );
    }
  } catch {
    const shares = (await listSharesByPeriod(period)).filter((item) => item.kind === kind);
    response = applyFallbackPageFilter(
      buildTrendResponse({
        period,
        view,
        kind,
        shares,
      }),
      { view, overallPage, yearPage }
    );
  }

  if (!sampleSummary) {
    sampleSummary = toSampleSummary(response);
    await setTrendSampleSummaryCache(period, kind, sampleSummary, TRENDS_STORE_CACHE_TTL_SECONDS);
  }

  response = applySampleSummary(response, sampleSummary);

  const normalizedResponse =
    response.sampleCount < 30
      ? {
          ...response,
          items: [],
        }
      : response;

  await setTrendsCache(period, view, kind, overallPage, yearPage, normalizedResponse, TRENDS_STORE_CACHE_TTL_SECONDS);
  return normalizedResponse;
}
