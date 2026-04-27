# INGO First-Close Tool — Makefile
# Local-first build orchestration. Eleventy + Python pipeline.

SHELL := /bin/bash
REPO := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
VENV := $(REPO)/.venv
PY := $(VENV)/bin/python
PIP := $(VENV)/bin/pip
ACTIVATE := source $(VENV)/bin/activate
# Eleventy installs into site/node_cache/ rather than the repo root so
# node_modules stays scoped to the static-site builder.
NPM_PREFIX := $(REPO)/site/node_cache
ELEVENTY := $(NPM_PREFIX)/node_modules/.bin/eleventy

export PYTHONPATH := $(REPO)

.PHONY: help install build build-private daily test fixtures-refresh clean

help:
	@echo "Targets:"
	@echo "  install           — pip install + npm install"
	@echo "  build             — PUBLIC build (no private pages)"
	@echo "  build-private     — PRIVATE=1 build (includes /private/*)"
	@echo "  daily             — scrape (fixtures) + emit + build + health roll-up"
	@echo "  test              — pytest"
	@echo "  fixtures-refresh  — live-GET each source, write snapshot-ok.*"
	@echo "  clean             — remove _site/, tool/cache/, __pycache__/"

install:
	@echo ">>> Creating venv at $(VENV) if missing"
	@[ -d $(VENV) ] || python3 -m venv $(VENV)
	@echo ">>> Installing Python deps into $(VENV)"
	$(PIP) install --upgrade pip
	$(PIP) install -e ".[dev]"
	@echo ">>> Installing Node deps into $(NPM_PREFIX)"
	mkdir -p $(NPM_PREFIX)
	npm --prefix $(NPM_PREFIX) install --no-audit --no-fund \
	    @11ty/eleventy@3.1.5 @11ty/eleventy-plugin-rss@2.0.3
	@echo ">>> install complete"

build:
	@echo ">>> PUBLIC build (PRIVATE unset)"
	rm -rf site/_site
	unset PRIVATE; NODE_PATH=$(NPM_PREFIX)/node_modules $(ELEVENTY) --input=site/src --output=site/_site

build-private:
	@echo ">>> PRIVATE build (PRIVATE=1)"
	rm -rf site/_site
	PRIVATE=1 NODE_PATH=$(NPM_PREFIX)/node_modules $(ELEVENTY) --input=site/src --output=site/_site

daily:
	@echo ">>> Daily run (INGO_FIXTURE_MODE=1)"
	INGO_FIXTURE_MODE=1 $(PY) -m pipeline.run
	INGO_FIXTURE_MODE=1 $(PY) -m pipeline.health --check-silence
	INGO_FIXTURE_MODE=1 $(PY) -m pipeline.build_slots
	$(MAKE) build
	INGO_FIXTURE_MODE=1 $(PY) -m pipeline.health --roll-up
	@if [ -n "$$(ls -A tool/health/open/ 2>/dev/null | grep -v .gitkeep)" ]; then \
	  echo "INCIDENTS OPEN"; \
	else \
	  echo "ALL GREEN"; \
	fi

test:
	$(PY) -m pytest -q

fixtures-refresh:
	@echo ">>> Refreshing live fixtures (one-shot, public GET-only)"
	INGO_LIVE=1 $(PY) -m pipeline.run --refresh-fixtures

clean:
	rm -rf site/_site
	rm -rf tool/cache/*
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	@echo "Cleaned."
