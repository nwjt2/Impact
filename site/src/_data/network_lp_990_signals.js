// ProPublica 990 sizing data for US foundations on the match page.
//
// Reads:  network/dashboard_prep/lp_990_signals.csv (built by
//         network/dashboard_prep/prep_scripts/combine_lp_990_signals.py)
// Emits:  { [lpSlug]: { ein, name, formType, latestFilingYear,
//                       totalAssetsUSD, annualGrantsPaidUSD,
//                       totalCharitableExpensesUSD, sourceUrl } }
//
// Sister to network_lp_summary.js. Loaded by site/src/match.njk to render a
// "Total assets ~$17B · 990 FY2023" line on cards for US foundations.
// Honest-null discipline: missing rows / blank fields silently omitted.

const fs = require("fs");
const path = require("path");

const CSV_PATH = path.resolve(
  __dirname,
  "..", "..", "..",
  "network", "dashboard_prep", "lp_990_signals.csv",
);

function parseCSV(text) {
  // Minimal CSV parse: assumes no embedded commas in fields (this CSV is
  // produced by csv_io.write_rows with default quoting; numeric fields are
  // unquoted). If we ever need quoted values with commas, swap to a real parser.
  const lines = text.replace(/\r\n/g, "\n").split("\n").filter((l) => l.length);
  if (lines.length === 0) return [];
  const headers = lines[0].split(",");
  return lines.slice(1).map((line) => {
    const cells = line.split(",");
    const row = {};
    headers.forEach((h, i) => { row[h] = cells[i] ?? ""; });
    return row;
  });
}

function toIntOrNull(s) {
  if (!s) return null;
  const n = parseInt(s, 10);
  return Number.isFinite(n) ? n : null;
}

module.exports = function () {
  if (!fs.existsSync(CSV_PATH)) {
    return {};  // pipeline hasn't run yet — empty signals, render path is null-safe
  }
  const rows = parseCSV(fs.readFileSync(CSV_PATH, "utf-8"));
  const out = {};
  rows.forEach((r) => {
    const slug = r["LP Slug"];
    if (!slug) return;
    out[slug] = {
      ein: r["EIN"] || null,
      name: r["Resolved Name"] || null,
      formType: r["Form Type"] || null,
      latestFilingYear: toIntOrNull(r["Latest Filing Year"]),
      totalAssetsUSD: toIntOrNull(r["Total Assets USD"]),
      annualGrantsPaidUSD: toIntOrNull(r["Annual Grants Paid USD"]),
      totalCharitableExpensesUSD: toIntOrNull(r["Total Charitable Expenses USD"]),
      sourceUrl: r["Source URL"] || null,
    };
  });
  return out;
};
