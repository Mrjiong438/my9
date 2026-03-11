"use client";

import { Gamepad2, Tv, Film, BookOpen, BookText, Library } from "lucide-react";
import { SubjectKind } from "@/lib/subject-kind";

export function SubjectKindIcon({
  kind,
  className,
}: {
  kind?: SubjectKind | null;
  className?: string;
}) {
  switch (kind) {
    case "game":
      return <Gamepad2 className={className} />;
    case "anime":
      return <Tv className={className} />;
    case "tv":
      return <Tv className={className} />;
    case "movie":
      return <Film className={className} />;
    case "manga":
      return <BookOpen className={className} />;
    case "lightnovel":
      return <BookText className={className} />;
    case "work":
    default:
      return <Library className={className} />;
  }
}
