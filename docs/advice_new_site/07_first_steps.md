# 07 — First Steps

A concrete, opinionated starting plan. Resist the urge to parallelize or skip ahead — getting one slice end-to-end is worth more than three half-built layers.

## Week 1 — Skeleton + first scraper end-to-end

**Goal: one fund's portfolio scraped, identified, registered, in SQLite, displayed on a page.**

1. **Repo init.** `requirements.txt` with `requests`, `beautifulsoup4`, `pandas`, `flask`, `pdfplumber`, `tldextract`. Python 3.10+. Set up venv.
2. **Directory skeleton** per [03_project_structure.md](03_project_structure.md). Empty placeholders are fine.
3. **`utils/`**: shared helpers — `slugify()`, `get_html()` with retry/UA/timeout, `csv_io.write_rows()`, `csv_io.read_rows()`. Build these before any scraper.
4. **Catalogue CSVs**: hand-create `catalogue/ingos.csv` (your INGO list) and `catalogue/impact_funds.csv` (one row to start; add the rest as you go).
5. **One custom portfolio scraper** for one fund whose portfolio page is straightforward HTML. Follow the template in [04_pipelines.md](04_pipelines.md). Make sure it sets `Scraping Method Used`.
6. **Combine + identify scripts** for `fund_portfolio_scraping`. Output `combined_fund_portfolios/run_1/all.csv`. Run a slug-registry pass on it.
7. **Latest-state scaffolding from day one**: write `dashboard_prep/timeline/state/fund_portfolio_state.json` and `base_registry_1.csv`. Skip the change detection step on run 1 (no previous run to diff against) but make sure run 2 will work.
8. **Webapp shell**: Flask app with a `/funds/<slug>` page that reads the SQLite DB. `import_data.py` reads the CSVs into SQLite. Tailwind compiled.
9. **End-to-end check**: scrape → combine → identify → state → import → view in browser. Once this works, you have a working pipeline. Everything else is filling it in.

**Critical**: do not move on until run 2 of the same scraper produces a meaningful change diff against run 1's base registry. This proves the latest-state plumbing is right.

## Week 2 — Second pipeline + a few more funds

**Goal: portco_investor_scraping working, three funds onboarded.**

1. Onboard 2–3 more funds into the portfolio pipeline. Each gets its own custom scraper.
2. Build the **portco_investor_scraping pipeline**. Same shape as fund_portfolio. Each portco gets a custom scraper for its "Investors" / "Backed by" section.
3. Update `import_data.py` and add `/companies/<slug>` and `/investors/<slug>` routes.
4. Write the **first version of `network_build.py`**: produce a JSON edge list of fund→portco and investor→portco. No fancy lead scoring yet.
5. Build a basic **`/network` page** with `cytoscape.js` — render the bipartite graph for one fund's portfolio + co-investors. Filter by fund.

## Week 3 — LP pipeline + lead scoring

**Goal: upstream layer captured for at least one fund. Lead-scoring view exists.**

1. **fund_lp_scraping pipeline.** This is the hardest pipeline because LP data is sparse. Start with the easy ones (funds that publish their LP list); fall back to PDF parsing of annual reports for the rest.
2. **`investors.csv` Impact Focus tagging.** Triage your top ~30 investors by hand. Mark each as `primary` / `mixed` / `none` / `unknown`. This is the analytic substrate for lead scoring.
3. **`lead_scoring.py`**. Implement the simple weighted formula from [05_dashboard_and_webapp.md](05_dashboard_and_webapp.md). Output `lead_scores.csv`.
4. **`/leads` page**: sortable table of investors with score, impact focus, # impact portcos co-invested in, # funds touched. CSV export.

## Week 4 — Polish + deploy

**Goal: live on a server, daily run cadence, first useful lead report generated.**

1. **`run_check`** scripts for each pipeline. Markdown reports.
2. **`run_all_scrape_batches.py`** orchestrator that runs all three pipelines in the right order.
3. **`deploy_data.sh`** with the runtime-outside-repo pattern.
4. **EC2 / Hetzner deploy**: nginx + gunicorn + systemd. Cert via certbot.
5. **First first-close prospect report**. Pick one INGO impact fund and produce a CSV of the top 50 generalist co-investors. This is the moment of truth for the product thesis.

## Anti-goals for v1

To stay focused, deliberately *don't* do these in v1:

- People/team scraping for funds or portcos.
- Round-by-round historical reconstruction (use latest cap table only).
- Auto-discovery of new INGO funds from search.
- Multi-user auth.
- Public-facing copy / SEO / marketing pages.
- Realtime data; daily runs are fine.
- Mobile UI.
- Generic scraper templates (write custom until you have 10+).

These are all reasonable v2 features. Adding any of them in v1 will add weeks and risk losing the core thesis.

## Definition of done for v1

- 10+ INGO impact funds onboarded.
- 80%+ of each fund's portfolio companies captured.
- 50%+ of those portcos have at least one co-investor recorded.
- LP rosters captured for at least 3 funds.
- Lead-scoring view shows ranked generalist investors.
- One INGO can run a "first close prospects" CSV export and find at least 5 names worth a meeting.
- Deployed and running daily.

If you hit this, the thesis is validated and you can confidently invest in v2 (people scraping, deeper round data, auto-discovery, etc.). If you don't hit it because the data is too sparse, that's an important signal too — the bottleneck is data sources, not engineering, and v2 should be about acquiring data (Crunchbase feed, manual research) not adding features.

## Working with Claude Code

A few patterns that work well:

- **Hand it [02_data_model.md](02_data_model.md) and [04_pipelines.md](04_pipelines.md) before asking for a new scraper.** It will get the conventions right the first time.
- **For a new fund onboarding, give it the fund's portfolio URL and ask it to write the custom scraper following the template.** Then ask it to run it and show you the output before you commit. Don't let it write 5 scrapers in one go.
- **Plan-then-execute mode** for anything touching dashboard prep or registry code. Get the plan right; the implementation follows.
- **For network/visualization changes, see them in the browser before committing.** Type checks pass != feature works.
- **Keep `discovery_learnings.md` updated.** Have Claude Code append a note every time it figures out a new scraping pattern. The doc compounds in value.

## After v1

Likely v2 priorities in rough order:

1. PDF parsing pipeline for INGO annual reports (LP rosters).
2. Crunchbase paid feed for portco co-investor coverage.
3. Auto-suggest "similar funds" for prospect reports.
4. Investor profile enrichment (AUM, sector focus, recent activity).
5. People scraping for funds (decision-makers — useful for outreach).
6. Operator review queue UI for new entity onboarding.
7. Strategy-cycle automation (recurring agents that propose new funds to onboard, surface stale data, etc).
