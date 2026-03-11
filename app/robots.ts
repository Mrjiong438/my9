import type { MetadataRoute } from "next";

export default function robots(): MetadataRoute.Robots {
  return {
    rules: [
      {
        userAgent: "*",
        allow: ["/", "/game", "/anime", "/tv", "/movie", "/manga", "/lightnovel", "/work"],
        disallow: ["/api/", "/trends", "/*/s/*"],
      },
    ],
  };
}
