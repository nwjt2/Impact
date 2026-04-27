"""Leak test: public build must not contain killed routes.

Route-level regression check — if any of the demolished paths re-appears
in the public build, fail.

Builds the public site (without PRIVATE) and walks site/_site/.
"""

import os
import platform
import shutil
import subprocess
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
SITE = REPO / "site" / "_site"
NPM_PREFIX = REPO / "site" / "node_cache"
ELEVENTY_BIN = NPM_PREFIX / "node_modules" / ".bin" / (
    "eleventy.cmd" if platform.system() == "Windows" else "eleventy"
)


# Killed routes — regenerating any of these is a regression.
KILLED_ROUTES = [
    "decide",
    "pipeline",
    "reference",
    "runbook",
    "private",
    "setup",
    "brief",
]

# Old page titles / unique strings that appeared only in dead routes.
# If they re-appear in site/_site, a killed feature snuck back in.
DEAD_STRINGS = [
    "Complete setup",
    "Log outreach on",
    "/brief/scoring",
    "named_target_lps",
    "parent_ingo_aliases",
    "outreach_log",
]


def test_public_build_has_no_killed_routes():
    env = os.environ.copy()
    env.pop("PRIVATE", None)
    env["NODE_PATH"] = str(NPM_PREFIX / "node_modules")
    shutil.rmtree(SITE, ignore_errors=True)
    r = subprocess.run(
        [str(ELEVENTY_BIN), "--input=site/src", "--output=site/_site"],
        cwd=str(REPO),
        capture_output=True,
        text=True,
        env=env,
    )
    assert r.returncode == 0, f"build failed:\nstdout:\n{r.stdout}\nstderr:\n{r.stderr}"
    assert SITE.exists(), "site/_site/ missing after build"

    for route in KILLED_ROUTES:
        p = SITE / route
        assert not p.exists(), f"public build contains killed route /{route}/ at {p}"


def test_public_build_has_no_dead_strings():
    assert SITE.exists(), "site/_site/ missing — run test_public_build_has_no_killed_routes first"
    offenders = []
    for p in SITE.rglob("*.html"):
        text = p.read_text(errors="replace")
        for s in DEAD_STRINGS:
            if s in text:
                offenders.append((str(p.relative_to(SITE)), s))
    assert not offenders, f"dead-feature residue found in public build: {offenders}"


def test_emitted_slot_jsons_have_no_user_scope_fields():
    """The three slot JSONs emitted by pipeline.build_slots are public by
    design, but they MUST NOT contain any field that only existed for
    the demolished user-config flow.
    """
    import json
    forbidden = [
        "named_target_lps",
        "sub_advisor_name",
        "placement_agent_name",
        "parent_ingo_aliases",
        "target_close_usd",
        "last_ddq_updated",
        "outreach_log",
        "user_fund",
    ]
    for name in ("peer_ingo_funds.json", "dfi_ingo_commits.json",
                 "deadlines.json", "slot_meta.json"):
        p = REPO / "site" / "src" / "_data" / name
        if not p.exists():
            continue  # pipeline not run yet — other tests cover schemas
        blob = p.read_text().lower()
        for k in forbidden:
            assert k.lower() not in blob, f"forbidden field `{k}` in {name}"
