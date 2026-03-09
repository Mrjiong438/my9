const SHARE_ID_PATTERN = /^[a-f0-9]{16}$/;
const SHARE_ID_PREFIX_PATTERN = /^([a-f0-9]{16})/;

function randomHex(bytes: number): string {
  const buffer = new Uint8Array(bytes);

  if (typeof globalThis.crypto !== "undefined" && globalThis.crypto.getRandomValues) {
    globalThis.crypto.getRandomValues(buffer);
  } else {
    for (let index = 0; index < buffer.length; index += 1) {
      buffer[index] = Math.floor(Math.random() * 256);
    }
  }

  return Array.from(buffer, (value) => value.toString(16).padStart(2, "0")).join("");
}

export function createShareId(): string {
  return randomHex(8);
}

export function normalizeShareId(value: string | null | undefined): string | null {
  if (!value) return null;
  const trimmed = value.trim().toLowerCase();
  if (SHARE_ID_PATTERN.test(trimmed)) {
    return trimmed;
  }

  const exactPrefix = trimmed.match(SHARE_ID_PREFIX_PATTERN);
  if (exactPrefix) {
    return exactPrefix[1];
  }

  if (!trimmed.includes("%")) {
    return null;
  }

  try {
    const decoded = decodeURIComponent(trimmed);
    const decodedPrefix = decoded.match(SHARE_ID_PREFIX_PATTERN);
    return decodedPrefix ? decodedPrefix[1] : null;
  } catch {
    return null;
  }
}

export function isCanonicalShareId(value: string | null | undefined): boolean {
  if (!value) return false;
  return SHARE_ID_PATTERN.test(value.trim().toLowerCase()) && value.trim().toLowerCase() === value.trim();
}

export function assertShareId(value: string | null | undefined): string {
  const normalized = normalizeShareId(value);
  if (!normalized) {
    throw new Error("invalid_share_id");
  }
  return normalized;
}
