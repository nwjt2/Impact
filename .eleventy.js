// Eleventy config. PRIVATE=1 enables /private/* pages.
// Default build is PUBLIC and excludes user/* + /private/*.

// Normalize an env-supplied path prefix: accepts "Impact", "/Impact", "Impact/",
// "/Impact/" — all become "/Impact/". Empty/unset returns "/".
function normalizePathPrefix(p) {
  if (!p) return "/";
  if (!p.startsWith("/")) p = "/" + p;
  if (!p.endsWith("/")) p = p + "/";
  return p;
}

module.exports = function (eleventyConfig) {
  const isPrivate = process.env.PRIVATE === "1";

  eleventyConfig.addGlobalData("env", {
    private: isPrivate,
    buildTime: new Date().toISOString(),
  });

  // Exclude /private/ from public builds by ignore rule.
  if (!isPrivate) {
    eleventyConfig.ignores.add("site/src/private/**");
  }

  // Passthrough static assets if they exist.
  eleventyConfig.addPassthroughCopy({ "site/src/assets": "assets" });

  // Handy filters.
  eleventyConfig.addFilter("dateISO", (d) => {
    try {
      return new Date(d).toISOString().slice(0, 10);
    } catch {
      return "";
    }
  });
  eleventyConfig.addFilter("truncate", (s, n = 160) => {
    if (!s) return "";
    s = String(s);
    return s.length > n ? s.slice(0, n - 1) + "…" : s;
  });
  eleventyConfig.addFilter("slice", (arr, start, end) => {
    if (!Array.isArray(arr)) return [];
    return arr.slice(start || 0, end);
  });
  eleventyConfig.addFilter("keys", (obj) => {
    if (!obj || typeof obj !== "object") return [];
    return Object.keys(obj);
  });
  eleventyConfig.addFilter("length", (v) => {
    if (v == null) return 0;
    if (Array.isArray(v) || typeof v === "string") return v.length;
    if (typeof v === "object") return Object.keys(v).length;
    return 0;
  });
  eleventyConfig.addFilter("sourceHost", (url) => {
    if (!url) return "";
    try {
      return new URL(url).host.replace(/^www\./, "");
    } catch {
      return String(url);
    }
  });

  return {
    dir: {
      input: "site/src",
      output: "site/_site",
      includes: "_includes",
      data: "_data",
    },
    templateFormats: ["njk", "md", "html"],
    markdownTemplateEngine: "njk",
    htmlTemplateEngine: "njk",
    pathPrefix: normalizePathPrefix(process.env.SITE_PATH_PREFIX),
  };
};
