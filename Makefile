# Audit Offloading Lakehouse — PoC entry points.
# Rule: every target only calls `docker compose ...` or `bash scripts/<x>.sh`, so each one has a
# plain equivalent without make (listed in README.md). Document targets with "## <text>".

SHELL := /bin/sh
.DEFAULT_GOAL := help

N ?= 1000
P ?= 10

.PHONY: help up down clean test demo load dbt docs

help: ## List available targets
	@bash scripts/help.sh

up: ## Build and start all services, wait until healthy (requires .env)
	docker compose up -d --build --wait

down: ## Stop services, keep volumes
	docker compose down

clean: ## Stop services and delete volumes (all local data)
	docker compose down -v

test: ## Run .NET tests in the pinned SDK image (atomicity test needs `make up`)
	bash scripts/test.sh

load: ## Generate load: make load N=5000 P=20 (defaults N=1000 P=10)
	docker compose run --rm loadgen --count $(N) --parallel $(P)

demo: ## End-to-end flow: load -> outbox -> kafka -> iceberg -> dbt -> gold, with reconciliation
	bash scripts/e2e_demo.sh

dbt: ## dbt build: staging/silver/gold models + data quality tests (via Trino)
	docker compose run --rm dbt build

docs: ## dbt docs generate (lineage + catalog) into lakehouse/dbt/target/
	docker compose run --rm dbt docs generate
