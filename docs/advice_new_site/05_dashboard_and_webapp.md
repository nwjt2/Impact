# 05 — Dashboard Prep & Webapp

This is where the network analysis happens, and where the hardest-won lessons live. Read [06_lessons_learned.md](06_lessons_learned.md) alongside this — it has the why behind several rules below.

## Latest-state workflow — do this from day one

A naive workflow produces a full snapshot of every entity each pipeline run, then computes changes by diffing snapshot N against snapshot N-1. That works at first and then collapses under its own weight: snapshots balloon, diff logic gets tangled with retroactive corrections, and migrating to a leaner model later costs months of careful work.

**Build the leaner model from day one.** Concretely:

### Per-pipeline state JSON

For each pipeline, maintain `dashboard_prep/timeline/state/<pipeline>_state.json`:

```json
{
  "as_of_run": 42,
  "as_of_date": "2026-05-12",
  "entities": {
    "<entity_uid>": {
      "fields": { "...": "..." },
      "first_seen_run": 12,
      "last_seen_run": 42
    }
  }
}
```

This is the point-in-time roster. It's the truth about "what is currently believed about each entity". It is the input to the next run's change detection.

### Base registry per run

For each pipeline, write `base_registry_<run>.csv` to `dashboard_prep/<pipeline>_field_registries/run_<N>/`. This is a flat CSV view of all known fields with as-of-run annotations. **Keep the last TWO runs of these CSVs**, not just the latest — change detection needs the previous run's `base_registry_<N-1>.csv`. Trying "latest only" causes the next pipeline run to abort because the diff input is gone.

### Append-only timeline

`dashboard_prep/timeline/timeline.csv` is the cumulative event log. One row per change event:

| Column | Notes |
|---|---|
| Event ID | UUID or `<run>_<entity>_<seq>` |
| Run Number | |
| Event Date | |
| Entity Type | `fund_lp` / `fund_investment` / `portco_investor` / `investor` / `portco` |
| Entity UID | |
| Event Type | `added` / `removed` / `field_changed` |
| Field | (for field_changed) |
| Old Value | |
| New Value | |
| Source URL | |
| Notes | |

**Append-only.** Never edit historical timeline rows. To correct mistakes, use `timeline_corrections.csv` (next).

### Timeline corrections

`dashboard_prep/timeline/timeline_corrections.csv` holds operator overrides for late-arriving info or scraper mistakes — e.g. "this row says investor X exited round Y on date Z, but actually they were never in round Y". Apply these as the last step of dashboard prep. Modeling corrections explicitly (rather than mutating the timeline) keeps the audit trail clean.

### Run metadata

`dashboard_prep/timeline/run_metadata.json` — per-run summary (run number, started/ended, scrape success counts, etc). Build this from the pipeline's own output, **not** by re-scanning raw logs. Retro-fitting raw-log scanning later is painful; skip the whole problem by writing this from pipeline output from day one.

## Network build

`network_build.py` produces a graph that the webapp loads. Output as JSON for the frontend (or normalized SQL tables, your call):

```json
{
  "nodes": [
    {"id": "investor:acme-capital", "type": "investor", "impact_focus": "none"},
    {"id": "fund:goodfund-impact", "type": "fund"},
    {"id": "company:bright-co", "type": "portco"}
  ],
  "edges": [
    {"source": "investor:acme-capital", "target": "fund:goodfund-impact", "kind": "lp"},
    {"source": "fund:goodfund-impact", "target": "company:bright-co", "kind": "investment"},
    {"source": "investor:global-capital", "target": "company:bright-co", "kind": "co-investor"}
  ]
}
```

Three edge kinds: `lp`, `investment`, `co-investor`. Different views in the webapp filter by edge kind.

## Lead scoring

`lead_scoring.py` produces `dashboard_prep/lead_scores.csv` — one row per investor, with:

| Column | Notes |
|---|---|
| Investor Slug | |
| Investor Name | |
| Impact Focus | from `investors.csv` |
| Impact Portcos Co-invested | count of distinct portcos where they co-invested with an INGO impact fund |
| Impact Funds Touched | count of distinct INGO impact funds whose portcos they overlap with |
| LP Of Impact Funds | count of INGO impact funds they are an LP of |
| Lead Score | weighted formula (see below) |
| Last Seen Date | |
| Dismissed | `Y` if in `dismissed_warm_leads.csv`, else blank |

A simple starting formula:

```
score = (3 if impact_focus == "none" else 1 if "mixed" else 0)
        * (impact_portcos_co_invested + 2 * lp_of_impact_funds)
```

Tune later. The point is: **generalists with deep co-investment overlap are the warmest leads**, and the formula needs to express that.

## Webapp

Stack: Flask, Jinja templates, SQLite (local), Tailwind, vanilla JS for graph rendering.

- Server-rendered HTML primary; lean on Flask routes + Jinja, not a SPA. For a small-audience research tool, this ages much better than a JS framework.
- Tailwind compiled via `npx tailwindcss -i static/input.css -o static/style.css --minify`. **Manual rebuild after changing template classes** — it's easy to forget; if your new utility class isn't taking effect, this is why.
- Network rendering: try `cytoscape.js` first. It handles 1k+ nodes well and supports filtering, layouts, and styling without a heavy framework. `d3-force` if you want full custom layout control later.

### Routes (suggested)

| Route | Page |
|---|---|
| `/` | Dashboard summary: # funds, # portcos, # investors, top warm leads |
| `/funds` | List of impact funds with basic metrics |
| `/funds/<slug>` | Fund detail: LPs, portfolio, co-investors |
| `/companies` | List of portcos |
| `/companies/<slug>` | Portco detail: cap table |
| `/investors` | List of investors with filters (impact focus, type, country) |
| `/investors/<slug>` | Investor detail: which funds they LP'd, which portcos they co-invested in |
| `/network` | Big network view with filters (fund subset, edge kind, impact focus) |
| `/leads` | Lead-scoring view with sort + export to CSV |
| `/leads/<fund_slug>` | First-close prospect report for a specific fund |

### `import_data.py`

CSV → SQLite. Reads from `catalogue/`, the relationship CSVs in `dashboard_prep/`, and the timeline. Idempotent — drop and rebuild every time. Don't try to incrementally maintain the DB; it's not worth the complexity at this scale.

### Local dev

```bash
python -m impact_tracker.webapp.import_data
python -m impact_tracker.webapp.app
# http://localhost:5000
```

Keep import as a separate command from running the server.

## Deploy

Take these deploy lessons seriously — each one was learned the hard way:

- **Mutable runtime data lives outside the repo on prod.** Use an env var like `IMPACT_TRACKER_DATA_ROOT=/home/ubuntu/impact_runtime/dashboard_prep`. The repo on prod stays clean — `git status` should be clean after every deploy.
- **`deploy_data.sh` should `git add --update`** for any tracked-but-now-deleted files in gitignored dirs. Without this, a hard-prune leaves the working tree dirty in ways that aren't obvious until you next deploy.
- **Don't nest the repo inside itself** on prod (`~/repo/repo`). Prod working dir is just `~/impact_tracker/`.
- **Verify `git status` is clean after deploy.** If it's dirty, that's a deploy bug, not normal state.

A Hetzner/EC2 box behind nginx + systemd (gunicorn) is plenty. Cert via certbot.

## Backup

CSVs are the source of truth. Back up the whole repo (or at least `catalogue/`, `dashboard_prep/`, all `*_scrape_history.csv`) to S3 or similar after every successful pipeline run. The SQLite DB is rebuildable; the CSVs are not.
