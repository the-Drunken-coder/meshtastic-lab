MESHTASTICATOR_COMMIT ?= $(shell test -z "$$(git status --porcelain)" && git rev-parse HEAD)
export MESHTASTICATOR_COMMIT

.PHONY: dev test lint gateway-spike integration-test acceptance browser-smoke clean require-source-revision

require-source-revision:
	@test -n "$(MESHTASTICATOR_COMMIT)" || { echo "Commit or stash local changes before building a provenance-bearing image." >&2; exit 1; }

dev: require-source-revision
	docker compose up --build

test:
	UV_CACHE_DIR=.uv-cache uv run pytest backend/tests/unit
	pnpm --dir frontend build

lint:
	UV_CACHE_DIR=.uv-cache uv run ruff check backend
	UV_CACHE_DIR=.uv-cache uv run mypy backend/app
	pnpm --dir frontend lint

gateway-spike:
	UV_CACHE_DIR=.uv-cache uv run pytest -m integration backend/tests/integration/test_gateway_spike.py

integration-test:
	UV_CACHE_DIR=.uv-cache uv run pytest -m integration backend/tests/integration

acceptance: require-source-revision
	UV_CACHE_DIR=.uv-cache uv run python scripts/acceptance.py

browser-smoke: require-source-revision
	bash scripts/smoke-test.sh

clean:
	rm -rf frontend/dist frontend/node_modules .mypy_cache .pytest_cache .ruff_cache .uv-cache
