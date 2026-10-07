#!/bin/sh
set -eu

cd "$(dirname "$0")/.."
project_name="async-payment-service-e2e-$$"
API_KEY="local-development-key"
export API_KEY

compose() {
  docker compose -f compose.yaml -f compose.e2e.yaml --project-name "$project_name" "$@"
}

cleanup() {
  exit_code=$?
  trap - EXIT INT TERM
  if [ "$exit_code" -ne 0 ]; then
    compose logs --no-color --tail=100 api consumer webhook-sink || true
  fi
  compose down --volumes --remove-orphans || true
  exit "$exit_code"
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
compose up --build --detach --wait --wait-timeout 180
E2E_API_URL="http://$(compose port api 8000)"
E2E_SINK_URL="http://$(compose port webhook-sink 8080)"
E2E_RABBIT_URL="http://$(compose port rabbitmq 15672)"
E2E_DATABASE_URL="postgresql://payments:payments@$(compose port postgres 5432)/payments"
E2E_COMPOSE_PROJECT="$project_name"
export E2E_API_URL E2E_SINK_URL E2E_RABBIT_URL E2E_DATABASE_URL E2E_COMPOSE_PROJECT
uv run python scripts/e2e.py
