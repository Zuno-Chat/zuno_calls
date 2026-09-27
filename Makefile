VENV := .venv
PY := $(VENV)/bin/python

$(VENV)/.ok: pyproject.toml
	python3 -m venv $(VENV)
	$(PY) -m pip install -q -e '.[dev]'
	touch $@

venv: $(VENV)/.ok

test: venv
	$(PY) -m pytest -q

check: venv
	$(VENV)/bin/ruff format --check src tests
	$(VENV)/bin/ruff check src tests
	$(VENV)/bin/mypy
	$(PY) -m pytest -q

# Needs Docker and a few minutes: builds a Synapse image with the module and
# runs tests/e2e against it. Never part of `make test`.
e2e: venv
	$(PY) -m pytest -q -m e2e tests/e2e

fmt: venv
	$(VENV)/bin/ruff format src tests
	$(VENV)/bin/ruff check --fix src tests

.PHONY: venv test check e2e fmt
