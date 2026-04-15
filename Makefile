# Convenience targets for building, testing, and running the library
# against each supported storage backend. The heavy lifting for the
# multi-backend test run lives in scripts/test-backends.sh.

GO        ?= go
GO_FLAGS  ?= -race -p=1
PKGS      := $(shell $(GO) list ./... | grep -v /examples/ | grep -v /ui/cmd/ | grep -v /ui/gen/)
COMPOSE   ?= docker compose -p sdj-test

POSTGRES_HOST_PORT ?= 15432
MYSQL_HOST_PORT    ?= 13306
POSTGRES_DSN := postgres://jobs:jobs@localhost:$(POSTGRES_HOST_PORT)/jobs_test?sslmode=disable
MYSQL_DSN    := root:jobs@tcp(localhost:$(MYSQL_HOST_PORT))/jobs_test?parseTime=true

.PHONY: help
help:
	@echo "Targets:"
	@echo "  test              Run the SQLite test suite (default)"
	@echo "  test-sqlite       Run tests against in-memory SQLite"
	@echo "  test-postgres     Run tests against the compose Postgres service"
	@echo "  test-mysql        Run tests against the compose MySQL service"
	@echo "  test-backends     Run tests against SQLite + Postgres + MySQL"
	@echo "  compose-up        Bring up Postgres and MySQL containers"
	@echo "  compose-down      Stop and remove test containers and volumes"
	@echo "  build             go build ./..."
	@echo "  lint              golangci-lint run"

.PHONY: test
test: test-sqlite

.PHONY: test-sqlite
test-sqlite:
	$(GO) test $(GO_FLAGS) $(PKGS)

.PHONY: test-postgres
test-postgres:
	TEST_DATABASE_URL='$(POSTGRES_DSN)' $(GO) test $(GO_FLAGS) $(PKGS)

.PHONY: test-mysql
test-mysql:
	TEST_MYSQL_URL='$(MYSQL_DSN)' $(GO) test $(GO_FLAGS) $(PKGS)

.PHONY: test-backends
test-backends:
	./scripts/test-backends.sh

.PHONY: compose-up
compose-up:
	$(COMPOSE) up -d --wait postgres mysql

.PHONY: compose-down
compose-down:
	$(COMPOSE) down -v

.PHONY: build
build:
	$(GO) build ./...

.PHONY: lint
lint:
	golangci-lint run --timeout=5m
