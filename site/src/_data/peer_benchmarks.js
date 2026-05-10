// Computed peer-fund benchmark statistics for /peer-benchmarks/.
//
// Honesty discipline: every cohort is reported with its n and the total
// it was selected from. Fields with 0% public-disclosure coverage are
// surfaced as findings ("0 of N funds disclose this"), not as gaps to
// hide. Distributions are computed only over funds where the field is
// non-null; the unmeasured remainder is named explicitly on the page.

const funds = require("./peer_ingo_funds.json");

function pctCoverage(populated, total) {
  if (!total) return 0;
  return Math.round((populated / total) * 100);
}

function median(sortedArr) {
  if (!sortedArr.length) return null;
  const m = Math.floor(sortedArr.length / 2);
  return sortedArr.length % 2 ? sortedArr[m] : (sortedArr[m - 1] + sortedArr[m]) / 2;
}

function tally(items) {
  const out = {};
  items.forEach((item) => {
    const k = item == null || item === "" ? "(not stated)" : item;
    out[k] = (out[k] || 0) + 1;
  });
  // Return sorted-by-count array of {label, count} for deterministic templating.
  return Object.entries(out)
    .map(([label, count]) => ({ label, count }))
    .sort((a, b) => b.count - a.count || a.label.localeCompare(b.label));
}

module.exports = function () {
  const total = funds.length;
  const verified = funds.filter((f) => f.validation_status === "verified").length;

  // Field-coverage table — drives the "what can/can't be benchmarked" framing.
  const coverageFields = [
    { key: "vintage", label: "Vintage year" },
    { key: "vehicle_type", label: "Vehicle type (closed-end / evergreen / DIB / blended)" },
    { key: "manager", label: "Fund manager / sub-advisor" },
    { key: "size_usd_m", label: "Fund target or closed size (USD)" },
    { key: "first_close_date", label: "First-close date" },
    { key: "anchor_lp", label: "Anchor LP (named)" },
    { key: "final_close_date", label: "Final-close date" },
    { key: "announce_date", label: "Announce-press date" },
    { key: "mgmt_fee_bps", label: "Management fee" },
    { key: "carry_pct", label: "Carry / performance fee" },
    { key: "hurdle_pct", label: "Hurdle rate" },
    { key: "gp_commit_pct", label: "GP commitment %" },
    { key: "investment_period_years", label: "Investment period (years)" },
  ];

  const coverage = coverageFields.map((f) => {
    const populated = funds.filter((x) => {
      const v = x[f.key];
      return v != null && v !== "" && !(Array.isArray(v) && v.length === 0);
    }).length;
    return {
      key: f.key,
      label: f.label,
      n: populated,
      pct: pctCoverage(populated, total),
    };
  });

  // Blended-finance sub-cohort coverage.
  const bfCapitalStack = funds.filter(
    (f) => f.blended_finance_structure && f.blended_finance_structure.capital_stack && f.blended_finance_structure.capital_stack.length,
  ).length;
  const bfParallelTA = funds.filter(
    (f) => f.blended_finance_structure && f.blended_finance_structure.parallel_ta_facility,
  ).length;

  // Size distribution.
  const sizes = funds.filter((f) => f.size_usd_m != null).map((f) => f.size_usd_m);
  sizes.sort((a, b) => a - b);
  const sizeBuckets = [
    { label: "Under $25M", lo: 0, hi: 25 },
    { label: "$25M – $50M", lo: 25, hi: 50 },
    { label: "$50M – $100M", lo: 50, hi: 100 },
    { label: "$100M – $250M", lo: 100, hi: 250 },
    { label: "$250M and up", lo: 250, hi: Infinity },
  ].map((b) => ({
    label: b.label,
    count: sizes.filter((s) => s >= b.lo && s < b.hi).length,
  }));

  // Vehicle distribution.
  const vehicles = tally(funds.map((f) => f.vehicle_type));

  // Vintage by 5-year bucket.
  const vintageBuckets = (() => {
    const out = {};
    funds.filter((f) => f.vintage).forEach((f) => {
      const lo = Math.floor(f.vintage / 5) * 5;
      const k = `${lo}–${lo + 4}`;
      out[k] = (out[k] || 0) + 1;
    });
    return Object.entries(out)
      .map(([label, count]) => ({ label, count }))
      .sort((a, b) => a.label.localeCompare(b.label));
  })();

  // Sector / geo prevalence (across all 94, multi-tag per fund).
  const sectors = tally(funds.flatMap((f) => f.sector_tags || []));
  const geos = tally(funds.flatMap((f) => f.geo_tags || []));

  // Status.
  const statuses = tally(funds.map((f) => f.status));

  // First-close dates: distribution by year.
  const firstCloseYears = tally(
    funds.filter((f) => f.first_close_date).map((f) => String(f.first_close_date).slice(0, 4)),
  ).sort((a, b) => a.label.localeCompare(b.label));

  return {
    total,
    verified,
    coverage,
    bfCapitalStack,
    bfParallelTA,
    size: {
      n: sizes.length,
      pct: pctCoverage(sizes.length, total),
      min: sizes[0],
      median: median(sizes),
      max: sizes[sizes.length - 1],
      buckets: sizeBuckets,
    },
    vehicles,
    vintageBuckets,
    sectors,
    geos,
    statuses,
    firstCloseYears,
    generatedAt: new Date().toISOString().slice(0, 10),
  };
};
