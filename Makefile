# Audit Offloading Lakehouse — PoC entry points.
# Every target is documented with "## <text>" so `make help` can list it.
# Targets are placeholders until the PLAN step that implements them.

SHELL := /bin/sh
.DEFAULT_GOAL := help

N ?= 3000
P ?= 20
FROM ?=

.PHONY: help up down clean demo load dbt maintenance chaos replay evolve lifecycle metrics

help: ## List available targets
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "}; {printf "  %-12s %s\n", $$1, $$2}'

up: ## Start all services (docker compose up -d) [step 01]
	@echo "up: not implemented yet (step 01)"

down: ## Stop services, keep volumes [step 01]
	@echo "down: not implemented yet (step 01)"

clean: ## Stop services and delete volumes (docker compose down -v) [step 01]
	@echo "clean: not implemented yet (step 01)"

load: ## Generate load: make load N=5000 P=20 [step 04]
	@echo "load: not implemented yet (step 04)"

demo: ## End-to-end flow: load -> outbox -> kafka -> iceberg -> dbt -> gold [step 06]
	@echo "demo: not implemented yet (step 06)"

dbt: ## Run dbt build (silver/gold models + tests) [step 06]
	@echo "dbt: not implemented yet (step 06)"

maintenance: ## Iceberg maintenance: compaction, snapshot expiry, orphans [step 07]
	@echo "maintenance: not implemented yet (step 07)"

evolve: ## Schema evolution v1 -> v2 + time travel demo [step 08]
	@echo "evolve: not implemented yet (step 08)"

lifecycle: ## Verified backfill to Iceberg + safe batched purge in SQL Server [step 09]
	@echo "lifecycle: not implemented yet (step 09)"

chaos: ## Resilience demo: ingestion and Kafka outages, zero loss [step 10]
	@echo "chaos: not implemented yet (step 10)"

replay: ## Replay from Kafka or outbox: make replay FROM=<iso-datetime> [step 10]
	@echo "replay: not implemented yet (step 10)"

metrics: ## Storage and latency cost-benefit metrics [step 12]
	@echo "metrics: not implemented yet (step 12)"
