# 06 — Lessons Learned

These are scars, not preferences. Each one cost real time to discover. Adopt them on day one.

## Data & pipeline

1. **`Scraping Method Used` is mandatory on every output row.** If missing, the combine step silently treats the row as a seed row and drops it. Add a unit test that asserts every scraper sets it. This is the #1 most common cause of "my row count went down for no reason".

2. **Slug registry is generated, never hand-edited.** Build `build_slug_registry.py` first, before you have many entities. Manual edits cause UID/slug mismatches that bite weeks later.

3. **Run numbering and `run_state.json` from day one.** Don't retrofit. A single repo-wide run counter beats per-pipeline counters — much easier to correlate timeline events.

4. **One catalogue per entity type, ONE.** Don't let multiple "list of funds" CSVs accumulate. The catalogue is the source of truth; everything else is derived.

5. **Two-run retention for registries**, not one. Change detection needs the previous run's `base_registry`. Trying "latest only" causes the next pipeline run to abort because the diff input is gone.

6. **Combined CSVs for the last two runs as belt-and-braces.** Cheap, and they save you when registry generation has a bug.

7. **Manual matches CSV is essential.** Fuzzy matching of investor/company names will be wrong 5–10% of the time. Build the operator escape hatch (`manual_matches.csv`) on day one.

8. **CSV header conventions**: title case with spaces (`"Fund Name"`), `Y`/`N`/`unknown` for booleans, `YYYY-MM-DD` for dates, pipe-separated lists. Pick the convention now and enforce it in tests.

9. **Skip lists save real time.** Every entity that fails scraping should land in `discovery_skip_list.csv` with a reason. Otherwise you re-try broken sources every run forever.

10. **Discovery learnings doc.** Free-text `docs/discovery_learnings.md`, updated every time you write a new scraper. Patterns repeat — Squarespace scroll triggers, Webflow `.collection-item`, lazy-loaded portfolio cards, etc. Future-you needs these.

## Scraper engineering

11. **One custom scraper per entity, until you've written 10+.** Then look at what genuinely repeats and consider a generic template. Premature generic scrapers cost more time than they save.

12. **Custom scrapers fail in custom ways.** A change to one fund's portfolio page only breaks one scraper. That's a feature, not a bug. Don't share too much state across them.

13. **Test with `requests` first, fall back to `playwright` only when JS is mandatory.** Playwright is 10–100x slower and adds a heavyweight dep.

14. **Single jump-scroll often doesn't trigger lazy-load.** Use incremental scroll for Squarespace, Webflow, and anything with intersection-observer-driven render. A single scroll-to-bottom can leave half the page unrendered.

15. **Parallel detail-page fetches need retry with backoff.** Transient network errors get swallowed silently in parallel try/except blocks, producing intermittent low row counts. Log failed URLs explicitly and retry with exponential backoff.

16. **Always log every URL fetched** to a per-scraper logfile. When the row count drops mysteriously, the URL log tells you whether the page changed or the network failed.

17. **`run_check` after every run.** Flag scrapers that produced 0 rows or <50% of last run. Don't fail the pipeline on warnings — just surface them.

## Latest-state workflow

18. **Don't build snapshot-everything-then-diff and migrate later.** Migrating from snapshot-diff to latest-state takes months. Build the latest-state model from day one: per-pipeline state JSON, base registry per run, append-only timeline, timeline corrections.

19. **Timeline is append-only.** Mistakes get corrected via `timeline_corrections.csv`, not by editing history. Keeps the audit trail intact.

20. **`run_metadata.json` from pipeline output, not from re-scanning logs.** Retrofitting raw-log scanning later is painful. Skip the whole problem by writing this from pipeline output from day one.

## Webapp

21. **Server-rendered HTML + Jinja is fine.** A SPA is overkill for a small-audience research tool. Server-rendered Flask ages well.

22. **Tailwind rebuild is manual** after changing template classes. Easy to forget; if your new class doesn't take effect, this is why. Document this in `WEBAPP_README.md`.

23. **Import is separate from serve.** `import_data.py` rebuilds SQLite from CSVs; `app.py` serves. Don't couple them.

24. **Drop-and-rebuild SQLite, don't incrementally maintain.** At this scale it's faster and simpler.

25. **`instance/*.db` and `.env` never go in git.** Add to gitignore on commit zero.

## Deploy

26. **Mutable runtime data lives outside the repo on prod.** Env var like `<APP>_DATA_ROOT=/home/ubuntu/runtime/...`. Repo stays clean.

27. **`git status` clean after every deploy** is a hard rule. Dirty tree after deploy = workflow bug, not normal.

28. **Don't nest the repo inside itself** on prod. `~/repo/repo` happens when deploy paths get confused.

29. **`deploy_data.sh` must `git add --update`** for tracked-but-now-deleted files in gitignored dirs. Without this, hard-prune cleanup leaves the working tree dirty in ways that aren't obvious until you next deploy.

30. **Hard-prune rehearsal before the first hard-prune.** Practice in a scratch copy. The retention policy you think is right ("keep latest only") is probably one run too aggressive.

## Process & operator workflow

31. **Three to five entities to start, not fifty.** Get one entity end-to-end through scrape → prep → webapp → deploy before adding more. Onboarding too many entities early eats weeks in operator review.

32. **Onboard with explicit operator review.** New entities discovered by pipeline land as `pending_onboard` in the catalogue. Don't auto-promote.

33. **Manual override CSVs are first-class.** `manual_matches.csv`, `manual_field_resolutions.csv`, `dismissed_warm_leads.csv`. Operator escape hatches are not optional.

34. **Document scraping decisions in `discovery_skip_list.csv` with reasons.** "Acme Fund: no portfolio page on website, only in annual report PDF — revisit when PDF parser is built."

35. **Backup CSVs to S3 after every successful pipeline run.** SQLite is rebuildable; CSVs are not. A 100MB nightly snapshot is cheap insurance.

## Things that look like good ideas but aren't

36. **Don't auto-write scrapers for new entities.** Templating tools that generate scraper boilerplate look great for the first 5; by entity 50 the bottleneck is operator review of the source page anyway. Write them by hand.

37. **Don't bundle changes into mega-commits.** Small, focused commits. Each scraper is its own commit. Each pipeline-prep change is its own commit. Bisecting is your friend later.

38. **Don't skip the discovery skip list.** "I'll remember this fund has no portfolio page" — you won't. Six runs later you'll re-try it.

39. **Don't try to model rounds perfectly in v1.** Round names, dates, and amounts are noisy. Treat round info as soft annotation, not a primary key.

40. **Don't build user accounts / multi-tenant in v1.** It's a single-operator research tool. Auth via nginx basic-auth or a reverse-proxy header is fine for years.
