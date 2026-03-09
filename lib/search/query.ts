function toAsciiLowercase(value: string): string {
  return value.replace(/[A-Z]/g, (char) => String.fromCharCode(char.charCodeAt(0) + 32));
}

export function normalizeSearchQuery(value: string | null | undefined): string {
  if (!value) return "";
  return toAsciiLowercase(value.trim().replace(/\s+/g, " "));
}
