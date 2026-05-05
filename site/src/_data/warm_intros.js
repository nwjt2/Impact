// Connective LPs — LPs that appear in 2+ impact funds in our scraped graph.
// These are the "bridge" nodes that can serve as warm intros between funds.
// Powered entirely from network.json (built from fund_lps.csv); no separate
// pipeline. The /connective-lps/ page renders the result; the warm-path
// overlay on /match-my-fund/ uses the same underlying graph.

const network = require("./network.json");

const ARCH_ORDER = [
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

const ARCH_LABELS = {
  dfi: "DFI",
  foundation: "Foundation",
  "family-office": "Family office",
  "asset-manager": "Asset manager / MIV",
  bank: "Bank",
  "pension-fund": "Pension fund",
  corporate: "Corporate",
  "bilateral-donor": "Bilateral donor",
  government: "Sovereign government",
  "cooperative-ngo": "NGO / cooperative",
  vc: "VC / private fund",
  other: "Other",
};

module.exports = function () {
  const fundIndex = new Map();
  network.nodes.forEach((n) => {
    if (n.type !== "fund") return;
    fundIndex.set(n.id, {
      slug: n.id.startsWith("fund:") ? n.id.slice(5) : n.id,
      name: n.name || n.id,
      ingo_slug: n.ingo_slug || null,
    });
  });

  const invMeta = new Map();
  network.nodes.forEach((n) => {
    if (n.type !== "investor") return;
    if (!n.id.startsWith("investor:")) return;
    invMeta.set(n.id.slice(9), {
      name: n.name || n.id.slice(9),
      archetype: n.investor_type || "other",
      country: n.country || null,
    });
  });

  const lpToFunds = new Map();
  network.edges.forEach((e) => {
    if (e.kind !== "lp") return;
    if (!e.source.startsWith("investor:")) return;
    const fund = fundIndex.get(e.target);
    if (!fund) return;
    const inv = e.source.slice(9);
    if (!lpToFunds.has(inv)) lpToFunds.set(inv, []);
    lpToFunds.get(inv).push({
      fund_slug: fund.slug,
      fund_name: fund.name,
      ingo_slug: fund.ingo_slug,
      source_url: e.source_url || null,
    });
  });

  // Build fund -> [LPs] for the co-LP rollup. For each bridge LP X,
  // every other LP Y that has appeared as an LP of any of X's funds is
  // someone X can warm-route to: the shared fund is the evidence.
  const fundToLps = new Map();
  lpToFunds.forEach((funds, lpSlug) => {
    funds.forEach((f) => {
      if (!fundToLps.has(f.fund_slug)) fundToLps.set(f.fund_slug, []);
      fundToLps.get(f.fund_slug).push({
        lp_slug: lpSlug,
        source_url: f.source_url,
        fund_name: f.fund_name,
      });
    });
  });

  const bridges = [];
  lpToFunds.forEach((funds, slug) => {
    if (funds.length < 2) return;
    const meta = invMeta.get(slug) || {
      name: slug,
      archetype: "other",
      country: null,
    };
    funds.sort((a, b) => (a.fund_name || "").localeCompare(b.fund_name || ""));

    // Dedupe co-LPs by slug; keep the first via-fund encountered as the
    // evidence (one piece of citable evidence is sufficient — additional
    // shared funds are summary stats, not new information for the UI).
    const coLpsBySlug = {};
    funds.forEach((f) => {
      const fundLps = fundToLps.get(f.fund_slug) || [];
      fundLps.forEach((other) => {
        if (other.lp_slug === slug) return;
        if (coLpsBySlug[other.lp_slug]) return;
        const otherMeta = invMeta.get(other.lp_slug) || {
          name: other.lp_slug,
          archetype: "other",
        };
        coLpsBySlug[other.lp_slug] = {
          slug: other.lp_slug,
          name: otherMeta.name,
          archetype: otherMeta.archetype,
          via_fund_slug: f.fund_slug,
          via_fund_name: f.fund_name,
          source_url: f.source_url || other.source_url || null,
        };
      });
    });
    const coLps = Object.values(coLpsBySlug).sort((a, b) =>
      (a.name || "").toLowerCase().localeCompare((b.name || "").toLowerCase())
    );

    // Archetype distribution of co-LPs — used for the per-card stacked bar.
    const distMap = {};
    coLps.forEach((c) => {
      distMap[c.archetype] = (distMap[c.archetype] || 0) + 1;
    });
    const archDist = ARCH_ORDER
      .filter((a) => distMap[a])
      .map((a) => ({
        key: a,
        label: ARCH_LABELS[a] || a,
        count: distMap[a],
      }));

    bridges.push({
      slug,
      name: meta.name,
      archetype: meta.archetype,
      country: meta.country,
      fund_count: funds.length,
      funds,
      coLps,
      coLpCount: coLps.length,
      archDist,
    });
  });

  bridges.sort((a, b) => {
    const ai = ARCH_ORDER.indexOf(a.archetype);
    const bi = ARCH_ORDER.indexOf(b.archetype);
    if (ai !== bi) return ai - bi;
    if (a.fund_count !== b.fund_count) return b.fund_count - a.fund_count;
    return (a.name || "").localeCompare(b.name || "");
  });

  const byArchetype = {};
  bridges.forEach((b) => {
    if (!byArchetype[b.archetype]) byArchetype[b.archetype] = [];
    byArchetype[b.archetype].push(b);
  });

  const archetypeGroups = ARCH_ORDER.filter((a) => byArchetype[a]).map((a) => ({
    key: a,
    label: ARCH_LABELS[a] || a,
    count: byArchetype[a].length,
    bridges: byArchetype[a],
  }));

  // Bridges ranked by reach (co-LP count) for the hero chart at top of page.
  const reachRanked = bridges.slice().sort((a, b) => {
    if (a.coLpCount !== b.coLpCount) return b.coLpCount - a.coLpCount;
    return (a.name || "").localeCompare(b.name || "");
  });
  const maxReach = reachRanked.length ? reachRanked[0].coLpCount : 0;
  const totalRoutes = bridges.reduce((acc, b) => acc + b.coLpCount, 0);

  return {
    bridges,
    archetypeGroups,
    reachRanked,
    maxReach,
    total: bridges.length,
    totalRoutes,
    fundReachTotal: bridges.reduce((acc, b) => acc + b.fund_count, 0),
    archetypeColors: {
      dfi: "#e25c5c",
      foundation: "#d6b94a",
      "family-office": "#b86bdf",
      "asset-manager": "#4ec5a6",
      bank: "#c97c3e",
      "pension-fund": "#7a8aab",
      corporate: "#8a7448",
      "bilateral-donor": "#2c6d9c",
      government: "#4a9cd6",
      "cooperative-ngo": "#94a64a",
      vc: "#3ed7b3",
      other: "#777",
    },
  };
};
