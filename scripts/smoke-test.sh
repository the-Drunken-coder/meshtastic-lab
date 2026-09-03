#!/usr/bin/env bash
set -euo pipefail

cleanup() {
  docker compose down
}
trap cleanup EXIT
docker compose up --build --detach --wait --wait-timeout 180

pnpm --dir frontend exec playwright install chromium
pnpm --dir frontend smoke
