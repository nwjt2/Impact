const network = require("./network.json");

const ARCHETYPE_ORDER = ["dfi", "foundation", "family-office", "vc", "government", "other"];

module.exports = function () {
  const investors = new Map();
  network.nodes.forEach((n) => {
    if (n.type === "investor") investors.set(n.id, n);
  });

  const lpFundCounts = new Map();
  const lpFundsCovered = new Set();
  network.edges.forEach((e) => {
    if (e.kind !== "lp") return;
    lpFundCounts.set(e.source, (lpFundCounts.get(e.source) || 0) + 1);
    lpFundsCovered.add(e.target);
  });

  const lpWall = [...lpFundCounts.entries()]
    .map(([id, count]) => {
      const inv = investors.get(id) || {};
      return {
        id,
        name: inv.name || id,
        archetype: inv.investor_type || "other",
        count,
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
    lpFundsTotal: network.stats.fund_count,
    lpArchetypeRollup,
    lpFeatured: featured,
    topCatalysts: (network.catalysts || []).slice(0, 3),
  };
};
