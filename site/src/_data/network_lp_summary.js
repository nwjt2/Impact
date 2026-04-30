// Shared network-LP rollup for the foundation, family-office, and DFI
// detail pages (and the home shelves). The registry-side
// known_ingo_gp_commits[] / ingo_gp_commits[] fields are sparse — they
// were populated by hand and only carry confirmed commitments to
// INGO-sponsored funds. The network LP edges, by contrast, come from
// the fund_lps.csv pipeline and every edge carries a public source URL,
// covering both INGO-sponsored funds and non-INGO comparables in the
// graph.
//
// Detail pages join on slug and surface the union, so an entity like
// Shell Foundation (0 registry commits, 11 network LP edges to
// comparable peer impact funds) is no longer rendered as "no
// commitments located."

const network = require("./network.json");

module.exports = function () {
  const fundIndex = new Map();
  network.nodes.forEach((n) => {
    if (n.type !== "fund") return;
    fundIndex.set(n.id, {
      id: n.id,
      slug: n.id.startsWith("fund:") ? n.id.slice(5) : n.id,
      name: n.name || n.id,
      ingo_backed: Boolean(n.ingo_slug),
    });
  });

  const lpFundsByInvestorSlug = new Map();
  network.edges.forEach((e) => {
    if (e.kind !== "lp") return;
    if (!e.source.startsWith("investor:")) return;
    const invSlug = e.source.slice(9);
    const fund = fundIndex.get(e.target);
    if (!fund) return;
    if (!lpFundsByInvestorSlug.has(invSlug)) {
      lpFundsByInvestorSlug.set(invSlug, []);
    }
    lpFundsByInvestorSlug.get(invSlug).push({
      fund_id: fund.id,
      fund_slug: fund.slug,
      fund_name: fund.name,
      ingo_backed: fund.ingo_backed,
      source_url: e.source_url || null,
    });
  });

  const lpCountBySlug = {};
  const lpFundsBySlug = {};
  lpFundsByInvestorSlug.forEach((funds, slug) => {
    const sorted = funds
      .slice()
      .sort((a, b) => a.fund_name.localeCompare(b.fund_name));
    lpCountBySlug[slug] = sorted.length;
    lpFundsBySlug[slug] = sorted;
  });

  return { lpCountBySlug, lpFundsBySlug };
};
