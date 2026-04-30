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
        edge: dict = {
            "source": f"investor:{lp_slug}",
            "target": f"fund:{fund_slug}",
            "kind": "lp",
        }
        if lp.get("Source URL"):
            edge["source_url"] = lp["Source URL"]
        if lp.get("Commitment Year"):
            edge["commitment_year"] = lp["Commitment Year"]
        edges.append(edge)
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

    # Catalysts: INGO-fund portcos that ALSO drew non-INGO capital.
    # The "INGO showcase" question: which investments magnetised co-capital,
    # and from which kinds of investors? Used by /catalysts/.
    fund_is_ingo = {n["id"]: bool(n.get("ingo_slug")) for n in nodes if n["type"] == "fund"}
    nodes_by_id = {n["id"]: n for n in nodes}

    portco_funds: dict[str, set[str]] = {}
    portco_coinvestors: dict[str, set[str]] = {}
    for e in edges:
        if e["kind"] == "investment":
            portco_funds.setdefault(e["target"], set()).add(e["source"])
        elif e["kind"] == "co-investor":
            portco_coinvestors.setdefault(e["target"], set()).add(e["source"])

    catalysts: list[dict] = []
    ingo_backed_portco_count = 0
    for portco_id, fund_ids in portco_funds.items():
        ingo_funds = [f for f in fund_ids if fund_is_ingo.get(f)]
        if not ingo_funds:
            continue
        ingo_backed_portco_count += 1
        non_ingo_fund_ids = [f for f in fund_ids if not fund_is_ingo.get(f)]
        coinv_ids = list(portco_coinvestors.get(portco_id, set()))
        if not non_ingo_fund_ids and not coinv_ids:
            continue
        pc = nodes_by_id[portco_id]
        ingo_fund_objs = [
            {"id": fid, "name": nodes_by_id[fid]["name"]}
            for fid in sorted(ingo_funds)
        ]
        non_ingo_fund_objs = [
            {"id": fid, "name": nodes_by_id[fid]["name"]}
            for fid in sorted(non_ingo_fund_ids)
        ]
        # Drop coinvestors whose slug matches an INGO fund already credited
        # for this portco. The same entity sometimes appears as both
        # `fund:<slug>` (the fund-side investment edge) and
        # `investor:<slug>` (when a portco's own investors page lists the
        # fund). Showing both makes the fund look like its own co-investor.
        ingo_fund_slugs = {fid.split(":", 1)[1] for fid in ingo_funds}
        coinv_objs = [
            {
                "id": cid,
                "name": nodes_by_id[cid]["name"],
                "archetype": nodes_by_id[cid].get("investor_type", "other"),
            }
            for cid in coinv_ids
            if cid.split(":", 1)[1] not in ingo_fund_slugs
        ]
        coinv_objs.sort(key=lambda x: x["name"])
        archetype_counts = Counter(c["archetype"] for c in coinv_objs)
        # Emit as list-of-records (not a dict) — Nunjucks templates can't
        # iterate dict items with key/value destructuring.
        archetype_counts_list = [
            {"archetype": a, "count": n}
            for a, n in sorted(archetype_counts.items(), key=lambda kv: -kv[1])
        ]
        catalysts.append(
            {
                "id": portco_id,
                "name": pc["name"],
                "website": pc.get("website") or "",
                "country": pc.get("country") or "",
                "ingo_funds": ingo_fund_objs,
                "non_ingo_funds": non_ingo_fund_objs,
                "coinvestors": coinv_objs,
                "non_ingo_total": len(non_ingo_fund_objs) + len(coinv_objs),
                "archetype_counts": archetype_counts_list,
            }
        )
    catalysts.sort(key=lambda c: (-c["non_ingo_total"], c["name"].lower()))

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
            "ingo_backed_portco_count": ingo_backed_portco_count,
            "catalyst_count": len(catalysts),
        },
        "catalysts": catalysts,
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
