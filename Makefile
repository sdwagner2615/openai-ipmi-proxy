UV ?= uv
CONFIG ?= ./config.yaml
IMAGE ?= relay

.PHONY: setup sync format lint typecheck test test-unit test-e2e coverage run build smoke clean

## Environment -----------------------------------------------------------------

setup: ## Create the venv and install everything (dev group)
	$(UV) venv
	$(UV) sync

sync: ## Re-install dependencies from uv.lock
	$(UV) sync

## Code quality ----------------------------------------------------------------

format: ## Auto-fix lint and format
	$(UV) run ruff check --fix .
	$(UV) run ruff format .

lint: ## Check only (what CI runs)
	$(UV) run ruff check .
	$(UV) run ruff format --check .

typecheck:
	$(UV) run mypy

## Tests -----------------------------------------------------------------------

test-unit: ## Unit suite (seconds, no subprocesses)
	$(UV) run pytest -m unit -q

test-e2e: ## E2E parity suite (real subprocesses; minutes)
	$(UV) run pytest -m e2e -q

test: lint typecheck test-unit test-e2e ## Full local gate (what CI runs)

coverage: ## Unit coverage report
	$(UV) run pytest -m unit -q --cov=relay --cov-report=term-missing

## Run / build -----------------------------------------------------------------

run: ## Run the proxy (CONFIG ?= ./config.yaml)
	$(UV) run relay --config $(CONFIG)

build: ## Build the Docker image
	docker build --build-arg SETUPTOOLS_SCM_PRETEND_VERSION=0.0.0+local -t $(IMAGE) .

smoke: build ## Build, boot the image with a noop config, check /healthz + /monitor/data
	docker rm -f relay-smoke 2>/dev/null || true
	docker run -d --name relay-smoke -p 8000:8000 \
	  -v ./smoke/config.yaml:/config/config.yaml:ro \
	  -e RELAY_CONFIG=/config/config.yaml $(IMAGE)
	for i in $$(seq 1 30); do \
	  curl -sf http://127.0.0.1:8000/healthz > /dev/null && break; \
	  sleep 1; \
	done
	curl -sf http://127.0.0.1:8000/healthz && echo
	curl -sf http://127.0.0.1:8000/monitor/data | $(UV) run python -c "import json,sys; d=json.load(sys.stdin); assert 'config' in d and 'sessions' in d" && echo "monitor data parses"
	docker rm -f relay-smoke > /dev/null

## Housekeeping ------------------------------------------------------------------

clean: ## Remove caches and coverage artifacts
	rm -rf .ruff_cache .mypy_cache .pytest_cache
	rm -rf $$(find . -name __pycache__ -not -path "./.venv/*")
	rm -f .coverage coverage.xml
