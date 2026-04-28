# Advice for the new Impact Investment Tracker repo

This folder contains opinionated advice for Claude Code in the new repo on how to structure an INGO-impact-investment tracker: a scrape → prep → dashboard pipeline that maps the investor network around INGO-affiliated impact funds.

## How to use these docs

Hand all of these to Claude Code in the new repo and have it read them in order before writing any code. They are intentionally opinionated — when in doubt, follow the advice; the alternatives have been tried and they cost months.

1. **[01_project_concept.md](01_project_concept.md)** — what we're building, why, and the core entity model.
2. **[02_data_model.md](02_data_model.md)** — entities, CSV schemas, identifiers, registries.
3. **[03_project_structure.md](03_project_structure.md)** — directory layout and pipeline pattern.
4. **[04_pipelines.md](04_pipelines.md)** — scraper conventions and the three pipelines.
5. **[05_dashboard_and_webapp.md](05_dashboard_and_webapp.md)** — prep, latest-state workflow, webapp design, network views.
6. **[06_lessons_learned.md](06_lessons_learned.md)** — hard-won rules. Do not skip.
7. **[07_first_steps.md](07_first_steps.md)** — concrete week-by-week starting tasks.

## The shape of this project

The product tracks **money flowing into and out of INGO-affiliated impact funds**. The unit of analysis is **investor relationships**, and the goal is lead generation for INGOs raising their first close. That implies:

- A four-layer entity model (INGO → Fund → PortCo → Investors), connected by edges that represent capital flow.
- Network analysis is the *primary* product, not a side analytic.
- LP/investor data is sparse and uneven; the scraping pipelines need to be robust to many small per-source quirks.
- The "people" angle (fund teams, portco teams) is secondary and should be skipped in v1.

Read [01_project_concept.md](01_project_concept.md) before anything else.
