# Audit Offloading Lakehouse — PoC entry points.
# Rule: every target only calls `docker compose ...` or `bash scripts/<x>.sh`, so each one has a
# plain equivalent without make (listed in README.md). Document targets with "## <text>".

SHELL := /bin/sh
.DEFAULT_GOAL := help

N ?= 1000
P ?= 10
FROM ?=

.PHONY: help up down clean test demo load dbt maintenance chaos replay evolve lifecycle metrics

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

demo: ## End-to-end flow: load -> outbox -> kafka -> iceberg -> dbt -> gold [step 06]
	@bash scripts/not_implemented.sh demo 06

dbt: ## Run dbt build (silver/gold models + tests) [step 06]
	@bash scripts/not_implemented.sh dbt 06

maintenance: ## Iceberg maintenance: compaction, snapshot expiry, orphans [step 07]
	@bash scripts/not_implemented.sh maintenance 07

evolve: ## Schema evolution v1 -> v2 + time travel demo [step 08]
	@bash scripts/not_implemented.sh evolve 08

lifecycle: ## Verified backfill to Iceberg + safe batched purge in SQL Server [step 09]
	@bash scripts/not_implemented.sh lifecycle 09

chaos: ## Resilience demo: ingestion and Kafka outages, zero loss [step 10]
	@bash scripts/not_implemented.sh chaos 10

replay: ## Replay from Kafka or outbox: make replay FROM=<iso-datetime> [step 10]
	@bash scripts/not_implemented.sh replay 10

metrics: ## Storage and latency cost-benefit metrics [step 12]
	@bash scripts/not_implemented.sh metrics 12
