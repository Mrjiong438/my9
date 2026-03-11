import { SubjectKind, getSubjectKindMeta } from "@/lib/subject-kind";
import { ShareSubject, SubjectSearchResponse } from "@/lib/share/types";

const TMDB_API_BASE_URL = "https://api.themoviedb.org/3";
const TMDB_API_READ_ACCESS_TOKEN = process.env.TMDB_API_READ_ACCESS_TOKEN;

// 动画 genre ID，用于过滤动画类电视剧
const TMDB_ANIMATION_GENRE_ID = 16;

// TMDB TV genre 中文映射表
const TMDB_TV_GENRE_ZH: Record<number, string> = {
  10759: "动作冒险",
  16: "动画",
  35: "喜剧",
  80: "犯罪",
  99: "纪录",
  18: "剧情",
  10751: "家庭",
  10762: "儿童",
  9648: "悬疑",
  10763: "新闻",
  10764: "真人秀",
  10765: "科幻奇幻",
  10766: "肥皂剧",
  10767: "脱口秀",
  10768: "战争政治",
  37: "西部",
};

// TMDB Movie genre 中文映射表
const TMDB_MOVIE_GENRE_ZH: Record<number, string> = {
  28: "动作",
  12: "冒险",
  16: "动画",
  35: "喜剧",
  80: "犯罪",
  99: "纪录",
  18: "剧情",
  10751: "家庭",
  14: "奇幻",
  36: "历史",
  27: "恐怖",
  10402: "音乐",
  9648: "悬疑",
  10749: "爱情",
  878: "科幻",
  10770: "电视电影",
  53: "惊悚",
  10752: "战争",
  37: "西部",
};

// TMDB Search TV API 返回的单个结果
type TmdbTvResult = {
  id: number;
  name: string;
  original_name: string;
  poster_path: string | null;
  first_air_date?: string;
  genre_ids?: number[];
  overview?: string;
};

// TMDB Search TV API 返回的分页结构
type TmdbSearchTvResponse = {
  page: number;
  total_pages: number;
  total_results: number;
  results: TmdbTvResult[];
};

// TMDB Search Movie API 返回的单个结果
type TmdbMovieResult = {
  id: number;
  title: string;
  original_title: string;
  poster_path: string | null;
  release_date?: string;
  genre_ids?: number[];
  overview?: string;
};

// TMDB Search Movie API 返回的分页结构
type TmdbSearchMovieResponse = {
  page: number;
  total_pages: number;
  total_results: number;
  results: TmdbMovieResult[];
};

function extractYear(raw?: string | null): number | undefined {
  if (!raw) return undefined;
  const year = Number.parseInt(raw.slice(0, 4), 10);
  if (!Number.isFinite(year) || year < 1970 || year > 2100) {
    return undefined;
  }
  return year;
}

function normalizeText(value: string): string {
  return value.trim().toLowerCase().replace(/\s+/g, "");
}

// 将 TMDB 结果转为项目统一的 ShareSubject 类型
function toShareSubject(result: TmdbTvResult): ShareSubject {
  const cover = result.poster_path
    ? `https://image.tmdb.org/t/p/w500${result.poster_path}`
    : null;

  const genres = Array.isArray(result.genre_ids)
    ? result.genre_ids
        .map((id) => TMDB_TV_GENRE_ZH[id])
        .filter((name): name is string => Boolean(name))
        .slice(0, 3)
    : [];

  return {
    id: result.id,
    name: result.original_name,
    localizedName:
      result.name !== result.original_name ? result.name : undefined,
    cover,
    releaseYear: extractYear(result.first_air_date),
    genres,
  };
}

// 将 TMDB 电影结果转为项目统一的 ShareSubject 类型
function toShareMovieSubject(result: TmdbMovieResult): ShareSubject {
  const cover = result.poster_path
    ? `https://image.tmdb.org/t/p/w500${result.poster_path}`
    : null;

  const genres = Array.isArray(result.genre_ids)
    ? result.genre_ids
        .map((id) => TMDB_MOVIE_GENRE_ZH[id])
        .filter((name): name is string => Boolean(name))
        .slice(0, 3)
    : [];

  return {
    id: result.id,
    name: result.original_title,
    localizedName:
      result.title !== result.original_title ? result.title : undefined,
    cover,
    releaseYear: extractYear(result.release_date),
    genres,
  };
}

// 过滤掉动画类电视剧
function isAnimationTv(result: TmdbTvResult): boolean {
  return Array.isArray(result.genre_ids) && result.genre_ids.includes(TMDB_ANIMATION_GENRE_ID);
}

function scoreCandidate(query: string, subject: ShareSubject): number {
  const q = normalizeText(query);
  if (!q) return 0;

  const candidates = [subject.localizedName || "", subject.name];
  let score = 0;

  for (const text of candidates) {
    const normalized = normalizeText(text);
    if (!normalized) continue;
    if (normalized === q) score += 100;
    if (normalized.startsWith(q)) score += 60;
    if (normalized.includes(q)) score += 25;
  }

  if (typeof subject.releaseYear === "number") {
    const yearText = String(subject.releaseYear);
    if (yearText.includes(q)) score += 5;
  }

  return score;
}

function createSuggestions(kind: SubjectKind): string[] {
  const kindLabel = getSubjectKindMeta(kind).label;
  return [
    `可尝试${kindLabel}正式名或别名`,
    "中日英名称切换检索通常更有效",
    "减少关键词，仅保留核心词",
  ];
}

export function buildTmdbSearchResponse(params: {
  query: string;
  kind: SubjectKind;
  items: ShareSubject[];
}): SubjectSearchResponse {
  const { query, kind, items } = params;

  const ranked = items
    .map((item) => ({
      id: item.id,
      score: scoreCandidate(query, item),
    }))
    .filter((item) => item.score > 0)
    .sort((a, b) => b.score - a.score)
    .slice(0, 3)
    .map((item) => item.id);

  const topPickIds =
    ranked.length > 0 ? ranked : items.slice(0, 2).map((item) => item.id);

  return {
    ok: true,
    source: "tmdb",
    kind,
    items,
    topPickIds,
    suggestions: createSuggestions(kind),
    noResultQuery: items.length === 0 && query.trim() ? query : null,
  };
}

// 调用 TMDB Search TV API
export async function searchTmdbTv(params: {
  query: string;
  kind: SubjectKind;
}): Promise<ShareSubject[]> {
  const { query } = params;
  const q = query.trim();
  if (!q) {
    return [];
  }

  if (!TMDB_API_READ_ACCESS_TOKEN) {
    throw new Error("TMDB_API_READ_ACCESS_TOKEN 未配置");
  }

  const searchUrl = new URL(`${TMDB_API_BASE_URL}/search/tv`);
  searchUrl.searchParams.set("query", q);
  searchUrl.searchParams.set("language", "zh-CN");
  searchUrl.searchParams.set("page", "1");

  const response = await fetch(searchUrl.toString(), {
    method: "GET",
    headers: {
      Accept: "application/json",
      Authorization: `Bearer ${TMDB_API_READ_ACCESS_TOKEN}`,
    },
    next: { revalidate: 0 },
  });

  if (!response.ok) {
    throw new Error(`TMDB search failed: ${response.status}`);
  }

  const json = (await response.json()) as TmdbSearchTvResponse;
  const results = Array.isArray(json?.results) ? json.results : [];

  // 过滤掉动画，转换为 ShareSubject
  const items = results
    .filter((result) => !isAnimationTv(result))
    .map(toShareSubject)
    .slice(0, 20);

  return items;
}

// 调用 TMDB Search Movie API
export async function searchTmdbMovie(params: {
  query: string;
  kind: SubjectKind;
}): Promise<ShareSubject[]> {
  const { query } = params;
  const q = query.trim();
  if (!q) {
    return [];
  }

  if (!TMDB_API_READ_ACCESS_TOKEN) {
    throw new Error("TMDB_API_READ_ACCESS_TOKEN 未配置");
  }

  const searchUrl = new URL(`${TMDB_API_BASE_URL}/search/movie`);
  searchUrl.searchParams.set("query", q);
  searchUrl.searchParams.set("language", "zh-CN");
  searchUrl.searchParams.set("page", "1");

  const response = await fetch(searchUrl.toString(), {
    method: "GET",
    headers: {
      Accept: "application/json",
      Authorization: `Bearer ${TMDB_API_READ_ACCESS_TOKEN}`,
    },
    next: { revalidate: 0 },
  });

  if (!response.ok) {
    throw new Error(`TMDB search failed: ${response.status}`);
  }

  const json = (await response.json()) as TmdbSearchMovieResponse;
  const results = Array.isArray(json?.results) ? json.results : [];

  const items = results
    .map(toShareMovieSubject)
    .slice(0, 20);

  return items;
}

