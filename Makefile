# Makefile for Bento - Docker Compose operations
# Simplifies common Docker operations for the Bento blockchain indexer

# Docker Compose files to use
COMPOSE_FILES = -f docker-compose.yml -f docker-compose.prod.yml

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
	@echo "  sql-cli          - Access PostgreSQL CLI"
	@echo "  help             - Show this help message"

.PHONY: build start restart gaps delete-old-data sql-cli help