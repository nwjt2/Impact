const network = require("./network.json");
const networkLpSummary = require("./network_lp_summary.js");

// Order matches investor_classifier.ARCHETYPES (Python). Keep in sync.
const ARCHETYPE_ORDER = [
  "dfi",
  "foundation",
  "family-office",
  "asset-manager",
  "bank",
  "pension-fund",
  "corporate",
  "bilateral-donor",
  "government",
  "cooperative-ngo",
  "vc",
  "other",
];

module.exports = function () {
  const { lpCountBySlug: networkLpCountBySlug } = networkLpSummary();
  const investors = new Map();
  network.nodes.forEach((n) => {
    if (n.type === "investor") investors.set(n.id, n);
  });

  const fundNames = new Map();
  const fundIsIngo = new Map();
  network.nodes.forEach((n) => {
    if (n.type === "fund") {
      fundNames.set(n.id, n.name || n.id);
      fundIsIngo.set(n.id, Boolean(n.ingo_slug));
    }
  });

  const lpFunds = new Map();
  const lpFundsCovered = new Set();
  // investor-id -> count of distinct LP'd funds (used to enrich foundation
  // and family-office shelves whose registry-side known_ingo_gp_commits is
  // sparse but whose entries appear in fund_lps.csv → network LP edges).
  const lpCountByInvestor = new Map();
  // Per-investor sets of (a) INGO-sponsored funds they LP and (b) comparable
  // funds they LP. Used to split the headline LP count: an INGO fund team
  // reading the home page should know how many of the named LPs have
  // actually committed to an INGO-sponsored vehicle vs. how many show up
  // only via non-INGO comparable funds.
  const lpInvestorIngoSet = new Set();
  const lpInvestorComparableSet = new Set();
  network.edges.forEach((e) => {
    if (e.kind !== "lp") return;
    if (!lpFunds.has(e.source)) lpFunds.set(e.source, []);
    lpFunds.get(e.source).push({
      name: fundNames.get(e.target) || e.target,
      source_url: e.source_url || null,
    });
    lpFundsCovered.add(e.target);
    lpCountByInvestor.set(e.source, (lpCountByInvestor.get(e.source) || 0) + 1);
    if (fundIsIngo.get(e.target)) lpInvestorIngoSet.add(e.source);
    else lpInvestorComparableSet.add(e.source);
  });

  let lpFundsCoveredIngo = 0;
  let lpFundsCoveredComparable = 0;
  lpFundsCovered.forEach((id) => {
    if (fundIsIngo.get(id)) lpFundsCoveredIngo += 1;
    else lpFundsCoveredComparable += 1;
  });

  const lpInvestorIngoCount = lpInvestorIngoSet.size;
  const lpInvestorComparableOnlyCount = [...lpInvestorComparableSet].filter(
    (id) => !lpInvestorIngoSet.has(id)
  ).length;

  const lpWall = [...lpFunds.entries()]
    .map(([id, funds]) => {
      const inv = investors.get(id) || {};
      const sortedFunds = funds.slice().sort((a, b) => a.name.localeCompare(b.name));
      return {
        id,
        name: inv.name || id,
        archetype: inv.investor_type || "other",
        count: sortedFunds.length,
        funds: sortedFunds,
        sourced: sortedFunds.every((f) => Boolean(f.source_url)),
      };
    })
    .sort((a, b) => b.count - a.count || a.name.localeCompare(b.name));

  const archMap = {};
  lpWall.forEach((l) => {
    archMap[l.archetype] = (archMap[l.archetype] || 0) + 1;
  });
  const lpArchetypeRollup = ARCHETYPE_ORDER
    .filter((a) => archMap[a])
    .map((a) => ({ archetype: a, count: archMap[a] }));

  const featured = lpWall.filter((l) => l.count >= 2).slice(0, 6);
  while (featured.length < 6 && featured.length < lpWall.length) {
    featured.push(lpWall[featured.length]);
  }

  return {
    lpWall,
    lpDistinctCount: lpWall.length,
    lpInvestorIngoCount,
    lpInvestorComparableOnlyCount,
    lpFundsCoveredCount: lpFundsCovered.size,
    lpFundsCoveredIngo,
    lpFundsCoveredComparable,
    lpFundsTotal: network.stats.fund_count,
    lpArchetypeRollup,
    lpFeatured: featured,
    topCatalysts: (network.catalysts || []).slice(0, 3),
    networkLpCountBySlug,
  };
};
