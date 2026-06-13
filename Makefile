# Makefile for Bento - Docker Compose operations
# Simplifies common Docker operations for the Bento blockchain indexer

# Docker Compose files to use
COMPOSE_FILES = -f docker-compose.yml -f docker-compose.prod.yml

# Database name to dump
DB_NAME ?= bento
# Output file for the dump (compressed custom format). Override with: make dump DUMP_FILE=...
DUMP_FILE ?= bento-$(shell date -u +%Y%m%d-%H%M%S).dump

# Default target
.DEFAULT_GOAL := help

# Build Docker images
build:
	docker compose $(COMPOSE_FILES) build

# Start services with force recreation
start:
	docker compose $(COMPOSE_FILES) up -d --force-recreate

# Restart all services
restart:
	docker compose $(COMPOSE_FILES) restart

# Run indexer gaps command to index missed blocks
gaps:
	docker compose $(COMPOSE_FILES) run indexer gaps

# Run delete-old-data command to clean up old data
delete-old-data:
	docker compose $(COMPOSE_FILES) run indexer delete-old-data

# Dump (backup) the database to a compressed custom-format file on the host.
# Restore with: pg_restore -U postgres -d $(DB_NAME) -j4 <file>
dump:
	docker compose $(COMPOSE_FILES) exec -T db pg_dump -U postgres -Fc $(DB_NAME) > $(DUMP_FILE)
	@echo "Wrote dump to $(DUMP_FILE)"

# Access PostgreSQL CLI
sql-cli:
	docker compose $(COMPOSE_FILES) exec -i db psql --user postgres

# Help target
help:
	@echo "Available targets:"
	@echo "  build            - Build Docker images"
	@echo "  start            - Start services with force recreation"
	@echo "  restart          - Restart all services"
	@echo "  gaps             - Run indexer gaps command to index missed blocks"
	@echo "  delete-old-data  - Run delete-old-data command to clean up old data"
	@echo "  dump             - Back up the database to a compressed dump file (DUMP_FILE=...)"
	@echo "  sql-cli          - Access PostgreSQL CLI"
	@echo "  help             - Show this help message"

.PHONY: build start restart gaps delete-old-data dump sql-cli help