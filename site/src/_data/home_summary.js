const network = require("./network.json");

const ARCHETYPE_ORDER = ["dfi", "foundation", "family-office", "vc", "government", "other"];

module.exports = function () {
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
  network.edges.forEach((e) => {
    if (e.kind !== "lp") return;
    if (!lpFunds.has(e.source)) lpFunds.set(e.source, []);
    lpFunds.get(e.source).push(fundNames.get(e.target) || e.target);
    lpFundsCovered.add(e.target);
  });

  let lpFundsCoveredIngo = 0;
  let lpFundsCoveredComparable = 0;
  lpFundsCovered.forEach((id) => {
    if (fundIsIngo.get(id)) lpFundsCoveredIngo += 1;
    else lpFundsCoveredComparable += 1;
  });

  const lpWall = [...lpFunds.entries()]
    .map(([id, funds]) => {
      const inv = investors.get(id) || {};
      const sortedFunds = funds.slice().sort((a, b) => a.localeCompare(b));
      return {
        id,
        name: inv.name || id,
        archetype: inv.investor_type || "other",
        count: sortedFunds.length,
        funds: sortedFunds,
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
    lpFundsCoveredCount: lpFundsCovered.size,
    lpFundsCoveredIngo,
    lpFundsCoveredComparable,
    lpFundsTotal: network.stats.fund_count,
    lpArchetypeRollup,
    lpFeatured: featured,
    topCatalysts: (network.catalysts || []).slice(0, 3),
  };
};
