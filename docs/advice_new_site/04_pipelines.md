# 04 — Scraping Pipelines

Three pipelines, all following the same shape. Each pipeline is independently runnable; they're chained by relationship not by code coupling.

| # | Pipeline | What it scrapes | Output |
|---|---|---|---|
| 1 | **fund_lp_scraping** | LPs / FoFs of each impact fund | `fund_lps.csv` |
| 2 | **fund_portfolio_scraping** | Portfolio companies of each impact fund | `fund_investments.csv` + new rows in `portfolio_companies.csv` |
| 3 | **portco_investor_scraping** | All investors in each portfolio company | `portco_investors.csv` + new rows in `investors.csv` |

Pipelines 2 and 3 *discover* new entities (companies, investors) that get added to the catalogue CSVs. Pipeline 1 mostly discovers new investor entities (LPs).

## Order of operations

For a given run N:

```
fund_portfolio_scraping  ──►  populates portfolio_companies.csv
                              ├──► then portco_investor_scraping (depends on portcos)
                              │
fund_lp_scraping  ───────────►  (independent of the above)
                              │
                              ▼
                       dashboard_prep
                       ├── identify (slug + ID registry pass)
                       ├── changes (per pipeline)
                       ├── timeline (append events)
                       ├── network_build (graph)
                       └── lead_scoring
```

`fund_lp_scraping` and `fund_portfolio_scraping` can run in parallel. `portco_investor_scraping` must run after `fund_portfolio_scraping` because it iterates over discovered portcos.

## Scraper conventions (CRITICAL)

These are not optional. Each one is a hard-won rule.

### Every scraper must

1. **Define a `SCRAPER_NAME` constant** at module top. Example: `SCRAPER_NAME = "fund_portfolio_scraper_AcmeImpact"`.
2. **Set `Scraping Method Used` on every output row** to the scraper name. If this column is missing, the combine step silently treats the row as a seed row and discards it. **This is the single most common scraper bug. Add a unit test for it.**
3. **Write to `individual_*/run_<N>/<EntitySlug>.csv`** — exactly one CSV per entity per run.
4. **Exit non-zero on hard failure** (network down, page schema changed) so the batch wrapper notices.
5. **Exit zero with an empty CSV** (just headers) when the page legitimately has zero results — distinct from a hard failure.
6. **Set a User-Agent** that identifies the bot and includes a contact URL. Don't pretend to be a browser.
7. **Sleep between requests** to the same host. Default 1–2s; tune per-source.

### Tooling baseline

- `requests` + `BeautifulSoup` for static HTML.
- `playwright` (sync API) only when JavaScript rendering is required. Test with requests first.
- `pdfplumber` for INGO annual report parsing (LP rosters often live here).
- `spaCy` (`en_core_web_sm`) only if you do people-name verification — for v1 you probably don't.
- `tldextract` to canonicalize websites for matching.
- One shared HTTP wrapper in `utils/http.py` with retries + backoff + UA + timeout.

### Scraper file template

```python
"""fund_portfolio_scraper_AcmeImpact.py

Scrapes Acme Impact Fund's portfolio page.
Source: https://acmeimpact.org/portfolio
"""
from utils.http import get_html
from utils.csv_io import write_rows
import bs4

SCRAPER_NAME = "fund_portfolio_scraper_AcmeImpact"
SOURCE_URL = "https://acmeimpact.org/portfolio"
FUND_SLUG = "acme-impact"          # must match catalogue/impact_funds.csv
INGO_SLUG = "acme-foundation"

def scrape(run_number: int, output_dir: str) -> int:
    html = get_html(SOURCE_URL)
    soup = bs4.BeautifulSoup(html, "html.parser")

    rows = []
    for card in soup.select(".portfolio-card"):
        company = card.select_one(".company-name").get_text(strip=True)
        rows.append({
            "Fund Slug": FUND_SLUG,
            "INGO Slug": INGO_SLUG,
            "Company Name": company,
            "Round": "",
            "Round Date": "",
            "Lead": "unknown",
            "Source URL": SOURCE_URL,
            "Source Date": "",         # filled by combine step
            "Scraping Method Used": SCRAPER_NAME,    # MANDATORY
        })

    write_rows(output_dir, f"{FUND_SLUG}.csv", rows)
    return len(rows)


if __name__ == "__main__":
    import sys
    run_n = int(sys.argv[1])
    out = sys.argv[2]
    print(scrape(run_n, out))
```

### Generic vs custom scrapers

You will be tempted to build a "generic" scraper that takes a CSV config (selectors, URLs) and works for many funds. **For v1, skip this and write one custom scraper per fund.** Generic templates break in non-obvious ways and you spend more time configuring them than writing each scraper directly. Add a generic path only after you've written 10+ custom ones and see the actual repeated pattern.

## The `run_check` step

After a pipeline run, run a check script that flags:

- Scrapers that produced 0 rows (vs >0 last run) — likely broken.
- Scrapers that produced <50% of last run's row count — possible breakage.
- Rows missing `Scraping Method Used`.
- Duplicate identifiers within a single run.
- Catalogue rows whose scraper file is missing.

Output a single Markdown report to `run_check/run_<N>_report.md`. Don't fail the pipeline on warnings — surface them for operator review.

## Scrape history CSVs

Each pipeline maintains an append-only `<pipeline>_scrape_history.csv` with one row per (run, entity, status). Columns:

| Column | Notes |
|---|---|
| Run Number | |
| Entity Slug | |
| Status | `success` / `empty` / `failed` / `skipped` |
| Row Count | |
| Notes | Reason for failure or skip |

This is your audit trail when something disappears mysteriously and you need to know "did we actually scrape this fund in run 23?"

## Discovery & onboarding

When `fund_portfolio_scraping` discovers a new portco, it appends to `portfolio_companies.csv` with `Status = pending_onboard`. An operator (or a slash command later) reviews and either:

- promotes to `active` and writes a custom scraper for it (if you'll deeply scrape its investors), or
- marks `Status = co-investors-only` (skip its own deep scraping; just record co-investors found via other channels), or
- adds to `discovery_skip_list.csv` with a reason.

Don't try to auto-write scrapers for new entities. Discovery tooling sounds appealing but the bottleneck is always operator review of which entity-pages are worth scraping. Build the manual flow first; automate it only after the manual flow is painful.

## Skip lists & learnings

- `docs/discovery_skip_list.csv` — companies/funds skipped, with reason (`no portfolio page`, `JS-only and behind login`, `dead site`, etc.). Saves re-trying every run.
- `docs/discovery_learnings.md` — accumulated free-text notes on scraping patterns: "Squarespace portfolio pages need scroll-to-render", "Webflow's `.collection-item` is the standard portfolio card", "INGO annual reports usually have LP rosters in the back-matter on a 2-column page", etc. Update this every time you write a new scraper. Future-you will thank you.

## Performance & politeness

- Cap concurrency per-host at 1 unless you've explicitly tested otherwise. The whole pipeline can be slow — that's fine.
- Set `timeout=30` on requests. Time out > hang.
- Log every URL fetched with status code to `logs/run_<N>/<scraper_name>.log`. Useful for postmortems.
- If a fund/company has rate limiting, switch to a generous delay (5s+) and run that scraper less often (e.g., monthly cadence in a separate batch group).
