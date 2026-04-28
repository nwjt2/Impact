"""Build the network graph JSON consumed by site/src/network.njk + cytoscape.js.

Reads from the catalogue + relationship CSVs and emits a single JSON file at
site/src/_data/network.json. Eleventy auto-loads it as the `network` variable
in templates.

Node types for v1:
  - ingo      (parent INGO, only those that sponsor a fund with edges)
  - fund      (only funds that appear in fund_investments.csv)
  - portco    (any portfolio company referenced by an edge)

Edge kinds:
  - sponsors    (ingo -> fund)
  - investment  (fund -> portco)

Future:
  - lp          (investor -> fund) — when fund_lps.csv lands
  - co-investor (investor -> portco) — when portco_investors.csv lands
"""
from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))

from network.utils.csv_io import read_rows  # noqa: E402

CATALOGUE = REPO_ROOT / "network" / "catalogue"
DASHBOARD_PREP = REPO_ROOT / "network" / "dashboard_prep"
OUTPUT_JSON = REPO_ROOT / "site" / "src" / "_data" / "network.json"
OUTPUT_JS = REPO_ROOT / "site" / "src" / "assets" / "js" / "network-data.js"


def build() -> dict:
    funds = {r["Fund Slug"]: r for r in read_rows(CATALOGUE / "impact_funds.csv")}
    ingos = {r["INGO Slug"]: r for r in read_rows(CATALOGUE / "ingos.csv")}
    portcos = {r["Company Slug"]: r for r in read_rows(CATALOGUE / "portfolio_companies.csv")}
    investors = {r["Investor Slug"]: r for r in read_rows(CATALOGUE / "investors.csv")}
    investments = read_rows(DASHBOARD_PREP / "fund_investments.csv")
    lps = read_rows(DASHBOARD_PREP / "fund_lps.csv")
    coinvestments = read_rows(DASHBOARD_PREP / "portco_investors.csv")

    edges: list[dict] = []
    fund_slugs_with_edges: set[str] = set()
    portco_slugs_with_edges: set[str] = set()
    portco_to_funds: dict[str, list[str]] = {}

    for inv in investments:
        fund_slug = inv.get("Fund Slug") or ""
        company_slug = inv.get("Company Slug") or ""
        if not fund_slug or not company_slug:
            continue
        edges.append(
            {
                "source": f"fund:{fund_slug}",
                "target": f"company:{company_slug}",
                "kind": "investment",
            }
        )
        fund_slugs_with_edges.add(fund_slug)
        portco_slugs_with_edges.add(company_slug)
        portco_to_funds.setdefault(company_slug, []).append(fund_slug)

    investor_slugs_with_edges: set[str] = set()
    for lp in lps:
        fund_slug = lp.get("Fund Slug") or ""
        lp_slug = lp.get("LP Slug") or ""
        if not fund_slug or not lp_slug:
            continue
        edges.append(
            {
                "source": f"investor:{lp_slug}",
                "target": f"fund:{fund_slug}",
                "kind": "lp",
            }
        )
        fund_slugs_with_edges.add(fund_slug)
        investor_slugs_with_edges.add(lp_slug)

    for ci in coinvestments:
        company_slug = ci.get("Company Slug") or ""
        investor_slug = ci.get("Investor Slug") or ""
        if not company_slug or not investor_slug:
            continue
        edges.append(
            {
                "source": f"investor:{investor_slug}",
                "target": f"company:{company_slug}",
                "kind": "co-investor",
            }
        )
        portco_slugs_with_edges.add(company_slug)
        investor_slugs_with_edges.add(investor_slug)

    ingo_slugs_with_edges: set[str] = set()
    for fund_slug in fund_slugs_with_edges:
        fund = funds.get(fund_slug)
        if not fund:
            continue
        ingo_slug = fund.get("INGO Slug") or ""
        if ingo_slug and ingo_slug in ingos:
            edges.append(
                {
                    "source": f"ingo:{ingo_slug}",
                    "target": f"fund:{fund_slug}",
                    "kind": "sponsors",
                }
            )
            ingo_slugs_with_edges.add(ingo_slug)

    nodes: list[dict] = []
    for slug in sorted(investor_slugs_with_edges):
        inv = investors.get(slug, {})
        nodes.append(
            {
                "id": f"investor:{slug}",
                "type": "investor",
                "name": inv.get("Investor Name") or slug,
                "investor_type": inv.get("Investor Type") or "other",
                "impact_focus": inv.get("Impact Focus") or "unknown",
                "country": inv.get("HQ Country") or "",
            }
        )
    for slug in sorted(ingo_slugs_with_edges):
        ingo = ingos[slug]
        nodes.append(
            {
                "id": f"ingo:{slug}",
                "type": "ingo",
                "name": ingo.get("INGO Name") or slug,
                "country": ingo.get("HQ Country") or "",
            }
        )
    for slug in sorted(fund_slugs_with_edges):
        fund = funds.get(slug, {})
        nodes.append(
            {
                "id": f"fund:{slug}",
                "type": "fund",
                "name": fund.get("Fund Name") or slug,
                "ingo_slug": fund.get("INGO Slug") or "",
                "fund_type": fund.get("Fund Type") or "unclassified",
                "thesis_tags": fund.get("Thesis Tags") or "",
            }
        )
    for slug in sorted(portco_slugs_with_edges):
        pc = portcos.get(slug, {})
        parent_funds = portco_to_funds.get(slug, [])
        nodes.append(
            {
                "id": f"company:{slug}",
                "type": "portco",
                "name": pc.get("Company Name") or slug,
                "country": pc.get("HQ Country") or "",
                "website": pc.get("Website") or "",
                "parent_fund_slug": parent_funds[0] if parent_funds else "",
                "parent_fund_count": len(parent_funds),
            }
        )

    edge_kinds = Counter(e["kind"] for e in edges)
    node_types = Counter(n["type"] for n in nodes)

    return {
        "nodes": nodes,
        "edges": edges,
        "stats": {
            "node_count": len(nodes),
            "edge_count": len(edges),
            "node_types": dict(node_types),
            "edge_kinds": dict(edge_kinds),
            "fund_count": node_types.get("fund", 0),
            "ingo_count": node_types.get("ingo", 0),
            "portco_count": node_types.get("portco", 0),
            "investor_count": node_types.get("investor", 0),
        },
    }


def main() -> None:
    graph = build()
    payload = json.dumps(graph, ensure_ascii=False, indent=2)

    OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_JSON.write_text(payload, encoding="utf-8")
    print(f"Wrote {OUTPUT_JSON}")

    # ALSO write a JS file the network page loads as <script src=...>.
    # Eleventy doesn't expose Nunjucks' `dump` filter, so we can't just
    # serialize `network` inline in the template. The JS-file approach
    # avoids that problem entirely.
    OUTPUT_JS.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_JS.write_text(
        f"window.NETWORK = {payload};\n",
        encoding="utf-8",
    )
    print(f"Wrote {OUTPUT_JS}")

    print(f"  nodes: {graph['stats']['node_count']} ({graph['stats']['node_types']})")
    print(f"  edges: {graph['stats']['edge_count']} ({graph['stats']['edge_kinds']})")


if __name__ == "__main__":
    main()
