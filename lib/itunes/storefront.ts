export type ItunesStorefront = "hk" | "jp" | "us";

const JAPANESE_KANA_RE = /[\u3040-\u30ff\u31f0-\u31ff\uff66-\uff9f]/;
const CJK_IDEOGRAPH_RE = /[\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff]/;

export function resolveItunesStorefrontForQuery(query: string): ItunesStorefront {
  const normalized = query.trim();
  if (!normalized) {
    return "us";
  }

  if (JAPANESE_KANA_RE.test(normalized)) {
    return "jp";
  }

  if (CJK_IDEOGRAPH_RE.test(normalized)) {
    return "hk";
  }

  return "us";
}
