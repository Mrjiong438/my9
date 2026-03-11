import { handleBangumiSearchRequest } from "@/lib/bangumi/route";
import { handleTmdbSearchRequest } from "@/lib/tmdb/route";
import { parseSubjectKind } from "@/lib/subject-kind";

// 使用 TMDB 作为数据源的 kind 集合
const TMDB_KINDS = new Set(["tv", "movie"]);

export async function GET(request: Request) {
  const { searchParams } = new URL(request.url);
  const kind = parseSubjectKind(searchParams.get("kind"));

  if (kind && TMDB_KINDS.has(kind)) {
    return handleTmdbSearchRequest(request);
  }
  return handleBangumiSearchRequest(request);
}
