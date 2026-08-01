#!/bin/sh
set -eu

project_name="async-payment-service-e2e"
compose_files="-f compose.yaml -f compose.e2e.yaml"
API_KEY="local-development-key"
export API_KEY

cleanup() {
  exit_code=$?
  if [ "$exit_code" -ne 0 ]; then
    docker compose $compose_files --project-name "$project_name" logs --no-color --tail=200 api consumer webhook-sink rabbitmq postgres
  fi
  docker compose $compose_files --project-name "$project_name" down --volumes --remove-orphans
  exit "$exit_code"
}

trap cleanup EXIT INT TERM
docker compose $compose_files --project-name "$project_name" up --build --detach --wait
uv run python scripts/e2e.py
