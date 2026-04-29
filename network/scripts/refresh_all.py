"""Refresh all network data: run every scraper, combine, rebuild JSON.

Auto-discovers scrapers under each pipeline's custom_minimal_*_scrapers/
folder. Continues past individual scraper failures (one site down should
not block the rest). Halts on combine or JSON-build failure (those are
deterministic and shouldn't fail unless something fundamental is broken).

After this completes successfully, run:
  - eleventy to rebuild the static site
  - git push to deploy

Usage:
  python -m network.scripts.refresh_all
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

SCRAPER_DIRS = [
    ("portfolio",
     REPO_ROOT / "network" / "fund_portfolio_scraping" / "portfolio_scrapers" / "custom_minimal_portfolio_scrapers"),
    ("lp",
     REPO_ROOT / "network" / "fund_lp_scraping" / "lp_scrapers" / "custom_minimal_lp_scrapers"),
    ("portco-investor",
     REPO_ROOT / "network" / "portco_investor_scraping" / "investor_scrapers" / "custom_minimal_investor_scrapers"),
    # LP-portfolio scrapers must run BEFORE combine_fund_lps and
    # combine_portco_investors so those combines pick up the secondary input.
    ("lp-portfolio",
     REPO_ROOT / "network" / "lp_portfolio_scraping" / "lp_portfolio_scrapers" / "custom_minimal_lp_portfolio_scrapers"),
]

COMBINE_MODULES = [
    "network.dashboard_prep.prep_scripts.combine_fund_portfolios",
    # combine_fund_lps and combine_portco_investors fold in LP-portfolio output
    # via secondary read paths (filtered by Investee Type).
    "network.dashboard_prep.prep_scripts.combine_fund_lps",
    "network.dashboard_prep.prep_scripts.combine_portco_investors",
]

BUILD_JSON_MODULE = "network.dashboard_prep.prep_scripts.build_network_json"


def discover_scrapers(scraper_dir: Path) -> list[str]:
    """Return scraper file stems (without .py) in this directory, excluding __init__."""
    if not scraper_dir.exists():
        return []
    return sorted(
        f.stem for f in scraper_dir.glob("*.py") if not f.stem.startswith("__")
    )


def run_module(module_path: str, args: list[str] | None = None, timeout: int = 300) -> tuple[bool, str]:
    """Run a Python module with `-m`. Returns (success, message)."""
    cmd = [sys.executable, "-m", module_path] + (args or [])
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        return False, f"TIMEOUT after {timeout}s"
    if result.returncode == 0:
        last = (result.stdout or "").strip().splitlines()
        return True, last[-1] if last else ""
    return False, (result.stderr or result.stdout)[-500:]


def main() -> None:
    run_number = 1  # TODO: read from run_state.json once Phase 6 (proper run-numbering) is wired
    print(f"=== Refresh all network data — run {run_number} ===\n")

    failures: list[str] = []

    # Pass 1: scrapers — continue on failure.
    for kind, scraper_dir in SCRAPER_DIRS:
        scrapers = discover_scrapers(scraper_dir)
        print(f"--- {kind.upper()} SCRAPERS ({len(scrapers)}) ---")
        if not scrapers:
            print("  (none)")
            print()
            continue
        rel = scraper_dir.relative_to(REPO_ROOT).as_posix().replace("/", ".")
        for name in scrapers:
            module = f"{rel}.{name}"
            print(f"  {name:60s} ", end="", flush=True)
            ok, msg = run_module(module, ["--run", str(run_number)])
            if ok:
                print("OK")
            else:
                print("FAIL")
                print(f"      {msg.strip()[:300]}")
                failures.append(f"{kind}/{name}")
        print()

    # Pass 2: combiners — halt on failure.
    print("--- COMBINE STEPS ---")
    for module in COMBINE_MODULES:
        name = module.rsplit(".", 1)[-1]
        print(f"  {name:60s} ", end="", flush=True)
        ok, msg = run_module(module, ["--run", str(run_number)])
        if ok:
            print("OK")
        else:
            print("FAIL")
            print(f"      {msg.strip()[:500]}")
            print("\nABORT: combine step failed. Fix the issue and re-run.")
            sys.exit(2)
    print()

    # Pass 3: build the network JSON.
    print("--- BUILD NETWORK JSON ---")
    print(f"  build_network_json{' ' * 42} ", end="", flush=True)
    ok, msg = run_module(BUILD_JSON_MODULE)
    if ok:
        print("OK")
    else:
        print("FAIL")
        print(f"      {msg.strip()[:500]}")
        sys.exit(2)
    print()

    # Summary
    print("=" * 60)
    if failures:
        print(f"Refresh complete with {len(failures)} scraper failure(s):")
        for f in failures:
            print(f"  - {f}")
        print()
        print("The combine + JSON-build steps still ran on whatever scraped successfully.")
        print("Investigate failures; skip-list any that can't be recovered.")
    else:
        print("Refresh complete — all scrapers succeeded.")
    print()
    print("Next: rebuild the static site and push.")
    print()
    print("  rm -rf site/_site")
    print("  NODE_PATH=site/node_cache/node_modules \\")
    print("    site/node_cache/node_modules/.bin/eleventy --input=site/src --output=site/_site")
    print("  git add -A && git commit -m 'Refresh network data' && git push origin main")


if __name__ == "__main__":
    main()
