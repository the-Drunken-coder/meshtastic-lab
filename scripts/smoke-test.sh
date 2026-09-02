#!/usr/bin/env bash
set -euo pipefail

docker compose up --build --detach
cleanup() {
  docker compose down
}
trap cleanup EXIT

pnpm --dir frontend exec playwright install chromium
pnpm --dir frontend smoke
