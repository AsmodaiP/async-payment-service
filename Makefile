.PHONY: install format lint test test-e2e up down logs

install:
	uv sync --all-groups

format:
	uv run ruff format .
	uv run ruff check --fix .

lint:
	uv run ruff format --check .
	uv run ruff check .
	uv run mypy .

test:
	uv run pytest -q --cov=payment_service

test-e2e:
	sh scripts/run-e2e.sh

up:
	docker compose up --build -d

down:
	docker compose down

logs:
	docker compose logs -f api consumer
