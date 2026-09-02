.PHONY: dev test lint gateway-spike integration-test acceptance browser-smoke clean

dev:
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

acceptance:
	UV_CACHE_DIR=.uv-cache uv run python scripts/acceptance.py

browser-smoke:
	bash scripts/smoke-test.sh

clean:
	rm -rf frontend/dist frontend/node_modules .mypy_cache .pytest_cache .ruff_cache .uv-cache
