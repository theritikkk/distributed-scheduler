.PHONY: build proto docker-up docker-down lint test concurrency-test db-init migrate migrate-up migrate-bootstrap migrate-status migrate-baseline migrate-rollback migrate-create archive-data

PYTHON ?= python3
DATABASE_URL ?= postgresql://scheduler:scheduler_secret@localhost:5432/distributed_scheduler
BASE_URL ?= http://localhost:3000
MIGRATE_TARGET ?=
ARCHIVE_RETENTION_DAYS ?= 30
ARCHIVE_BATCH_SIZE ?= 500
ARCHIVE_MAX_BATCHES ?= 100
ARCHIVE_DRY_RUN ?=
MIGRATE := $(PYTHON) scripts/migrate.py
ARCHIVE := $(PYTHON) scripts/archive_data.py

build:
	cd api-gateway && npm ci && npm run build
	$(PYTHON) -m compileall -q coordinator worker scripts

proto:
	@which protoc >/dev/null || (echo "install protoc and run: protoc --proto_path=proto --go_out=proto --go_opt=module=github.com/distributed-scheduler/proto --go-grpc_out=proto --go-grpc_opt=module=github.com/distributed-scheduler/proto proto/scheduler.proto" && exit 1)
	protoc --proto_path=proto --go_out=proto --go_opt=module=github.com/distributed-scheduler/proto --go-grpc_out=proto --go-grpc_opt=module=github.com/distributed-scheduler/proto proto/scheduler.proto

docker-up:
	docker compose up -d

docker-down:
	docker compose down

lint:
	cd api-gateway && npm run lint
	$(PYTHON) -m ruff check coordinator worker scripts tests

test:
	cd api-gateway && npm test
	cd coordinator && $(PYTHON) -m pytest tests
	cd worker && $(PYTHON) -m pytest tests
	$(PYTHON) -m pytest tests

concurrency-test:
	$(PYTHON) tests/concurrency/task_submission.py --base-url "$(BASE_URL)"

db-init:
	DATABASE_URL="$(DATABASE_URL)" $(PYTHON) scripts/init_db.py

migrate: migrate-bootstrap

migrate-bootstrap:
	DATABASE_URL="$(DATABASE_URL)" $(MIGRATE) bootstrap $(if $(MIGRATE_TARGET),--to "$(MIGRATE_TARGET)",)

migrate-up:
	DATABASE_URL="$(DATABASE_URL)" $(MIGRATE) up $(if $(MIGRATE_TARGET),--to "$(MIGRATE_TARGET)",)

migrate-status:
	DATABASE_URL="$(DATABASE_URL)" $(MIGRATE) status

migrate-baseline:
	DATABASE_URL="$(DATABASE_URL)" $(MIGRATE) baseline

migrate-rollback:
	DATABASE_URL="$(DATABASE_URL)" $(MIGRATE) rollback

migrate-create:
	@test -n "$(NAME)" || (echo "usage: make migrate-create NAME=add_feature" && exit 1)
	DATABASE_URL="$(DATABASE_URL)" $(MIGRATE) create "$(NAME)"

archive-data:
	DATABASE_URL="$(DATABASE_URL)" $(ARCHIVE) \
		--retention-days $(ARCHIVE_RETENTION_DAYS) \
		--batch-size $(ARCHIVE_BATCH_SIZE) \
		--max-batches $(ARCHIVE_MAX_BATCHES) \
		$(if $(ARCHIVE_DRY_RUN),--dry-run,)
