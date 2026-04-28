# 03 — Project Structure

Use a three-layer structure — scraping pipelines + dashboard prep + webapp — with names that fit the impact-investing domain. Each pipeline is self-contained.

## Suggested layout

```
impact_tracker/
├── catalogue/                          # Top-level catalogue CSVs (hand + script curated)
│   ├── ingos.csv
│   ├── impact_funds.csv
│   ├── investors.csv
│   └── portfolio_companies.csv
│
├── fund_lp_scraping/                   # Pipeline 1: LPs of impact funds (UPSTREAM)
│   ├── lp_scrapers/
│   │   ├── custom_minimal_lp_scrapers/    # bespoke per-fund scrapers
│   │   └── generic_lp_scraper.py          # template for hand-curated LP lists
│   ├── run_prep/
│   ├── run_check/
│   ├── individual_fund_lps/run_N/         # gitignored
│   ├── combined_fund_lps/run_N/           # gitignored
│   ├── fund_lp_scrape_history.csv         # append-only coverage log
│   ├── batch_scrape.py
│   └── run_lp_pipeline.py
│
├── fund_portfolio_scraping/            # Pipeline 2: portcos of impact funds
│   ├── portfolio_scrapers/
│   │   ├── custom_minimal_portfolio_scrapers/
│   │   └── generic_portfolio_scraper.py
│   ├── run_prep/
│   ├── run_check/
│   ├── individual_fund_portfolios/run_N/  # gitignored
│   ├── combined_fund_portfolios/run_N/    # gitignored
│   ├── fund_portfolio_scrape_history.csv
│   ├── batch_scrape.py
│   └── run_portfolio_pipeline.py
│
├── portco_investor_scraping/           # Pipeline 3: co-investors in portcos (DOWNSTREAM)
│   ├── investor_scrapers/
│   │   ├── custom_minimal_investor_scrapers/
│   │   └── generic_investor_scraper.py
│   ├── run_prep/
│   ├── run_check/
│   ├── individual_portco_investors/run_N/ # gitignored
│   ├── combined_portco_investors/run_N/   # gitignored
│   ├── portco_investor_scrape_history.csv
│   ├── batch_scrape.py
│   └── run_investor_pipeline.py
│
├── dashboard_prep/                     # Post-scrape processing
│   ├── prep_scripts/
│   │   ├── build_id_registry.py
│   │   ├── build_slug_registry.py
│   │   ├── fund_lp_changes.py
│   │   ├── fund_investment_changes.py
│   │   ├── portco_investor_changes.py
│   │   ├── network_build.py             # the bipartite/tripartite graph build
│   │   ├── lead_scoring.py              # generalist-vs-impact lead ranker
│   │   └── apply_timeline_corrections.py
│   ├── timeline/
│   │   ├── state/                       # per-pipeline latest-state JSON
│   │   ├── timeline.csv                 # append-only event log
│   │   ├── timeline_corrections.csv     # operator overrides
│   │   └── run_metadata.json
│   ├── master_id_registry.csv
│   ├── master_slug_registry.csv
│   ├── manual_matches.csv
│   ├── manual_historical_matches.csv
│   ├── manual_field_resolutions.csv
│   ├── dismissed_warm_leads.csv
│   └── changes_*/                       # per-pipeline change diffs per run
│
├── webapp/                             # Flask + SQLite + Tailwind
│   ├── app.py
│   ├── import_data.py                   # CSV → SQLite import
│   ├── routes/
│   │   ├── funds.py
│   │   ├── investors.py
│   │   ├── companies.py
│   │   ├── network.py                   # network views
│   │   └── leads.py                     # lead-scoring views
│   ├── templates/
│   ├── static/
│   │   ├── input.css
│   │   ├── style.css                    # built by tailwindcss
│   │   └── network.js                   # cytoscape.js or similar
│   └── instance/                        # gitignored; vc_dashboard.db
│
├── deploy/                             # EC2/server config
├── scripts/                            # one-off and operator helpers
├── tests/                              # pytest
├── docs/
│   ├── PIPELINE_GUIDE.md
│   ├── WEBAPP_README.md
│   ├── EC2_DEPLOYMENT.md
│   ├── discovery_learnings.md           # accumulated scraping pattern notes
│   └── discovery_skip_list.csv
├── config/
├── utils/                              # shared helpers (slugify, fetch, etc.)
├── run_state.json                       # current run number
├── run_all_scrape_batches.py
├── deploy_data.sh
├── requirements.txt
├── CLAUDE.md
└── README.md
```

## Pipeline pattern (each pipeline follows this shape)

1. **prep** — generate seed files from the catalogue CSV.
2. **scrape** — run per-entity scrapers; output to `individual_*/run_N/`.
3. **combine** — merge per-entity CSVs into `combined_*/run_N/`.
4. **check** — validate row counts, dropped rows, schema, zero-output failures.
5. **identify** — apply ID/slug registries.
6. **state + base registry** — write per-pipeline `state/<pipeline>_state.json` and `base_registry_<run>.csv`.
7. **changes** — diff against previous run's base registry to produce a change log.
8. **timeline** — append change events to the cumulative timeline.
9. **corrections** — apply operator corrections from `timeline_corrections.csv`.

This is the "latest-state workflow", and you should build it this way from day one. See [05_dashboard_and_webapp.md](05_dashboard_and_webapp.md) for the why.

## Run numbering

A single `run_state.json` at the repo root tracks the current run number. All three pipelines share it — a "run 12" produces run 12 outputs in every pipeline that ran. This makes timeline correlation trivial. Don't give each pipeline its own run counter.

## What to put in `.gitignore`

- `**/individual_*/run_*/`
- `**/combined_*/run_*/`
- `dashboard_prep/changes_*/`
- `dashboard_prep/timeline/state/*.json` if you want them out (recommended: keep them tracked in git for timeline reproducibility)
- `webapp/instance/`
- `.env`
- `__pycache__/`, `*.pyc`
- `venv/`, `.venv/`
- `node_modules/`

Catalogue CSVs, master registries, manual override CSVs, timeline.csv, timeline_corrections.csv, scrape_history.csv files: **always tracked in git**. These are the source of truth.

## Naming conventions

- Pipeline directories: `<role>_scraping/`.
- Scraper files: `<pipeline>_scraper_<EntitySlug>.py`. Example: `fund_portfolio_scraper_AcmeImpact.py`.
- The `<EntitySlug>` portion **must** match the slug in the corresponding catalogue CSV — pipelines look up scrapers by this convention.
- Python: snake_case files and functions; PascalCase classes.
- CSV headers: title case with spaces.
