.PHONY: test test-unit test-integration test-postgres test-mysql test-sqlite test-all help coverage \
	lint fmt fmt-fix vet tools bench ci ci-quick clean check-docker

PODMAN_SOCKET := $(shell if [ -S /run/user/$(shell id -u)/podman/podman.sock ]; then echo "unix:///run/user/$(shell id -u)/podman/podman.sock"; fi)
ifneq ($(PODMAN_SOCKET),)
    export DOCKER_HOST=$(PODMAN_SOCKET)
    export TESTCONTAINERS_RYUK_DISABLED=true
    CONTAINER_ENGINE=Podman
else
    CONTAINER_ENGINE=Docker
endif


help:
	@echo "Queen - Database Migration Tool"
	@echo ""
	@echo "Available targets:"
	@echo "  make test              - Run unit tests only (fast, no Docker)"
	@echo "  make test-integration  - Run all integration tests (requires Docker/Podman)"
	@echo "  make test-postgres     - Run Postgres integration tests"
	@echo "  make test-mysql        - Run MySQL integration tests"
	@echo "  make test-sqlite       - Run SQLite integration tests"
	@echo "  make test-all          - Run both unit and integration tests"
	@echo "  make coverage          - Generate test coverage report"
	@echo "  make lint              - Run golangci-lint"
	@echo "  make fmt               - Check code formatting"
	@echo "  make fmt-fix           - Format code with gofmt"
	@echo "  make vet               - Run go vet"
	@echo "  make bench             - Run benchmarks"
	@echo "  make tools             - Install development tools"
	@echo "  make ci-quick          - Run quick CI checks (fmt, vet, lint, test)"
	@echo "  make ci                - Run full CI (all checks + integration tests)"
	@echo "  make clean             - Clean test artifacts"
	@echo "  make install-deps      - Install testcontainers-go dependency"
	@echo ""
	@echo "Detected container engine: $(CONTAINER_ENGINE)"
ifneq ($(PODMAN_SOCKET),)
	@echo "  DOCKER_HOST=$(DOCKER_HOST)"
	@echo "  TESTCONTAINERS_RYUK_DISABLED=$(TESTCONTAINERS_RYUK_DISABLED)"
endif
	@echo ""

test:
	@echo "==> Running unit tests..."
	go test -v -race ./...

test-unit: test

test-integration:
	@echo "==> Running integration tests (using $(CONTAINER_ENGINE))..."
	@echo "    This will download container images on first run (~500MB)"
	go test -v -tags=integration -timeout=15m ./drivers/postgres ./drivers/mysql ./drivers/sqlite ./drivers/cockroachdb ./drivers/clickhouse ./drivers/mssql ./tests/integration

test-postgres:
	@echo "==> Running Postgres integration tests (using $(CONTAINER_ENGINE))..."
	go test -v -tags=integration -timeout=5m ./drivers/postgres

test-mysql:
	@echo "==> Running MySQL integration tests (using $(CONTAINER_ENGINE))..."
	go test -v -tags=integration -timeout=5m ./drivers/mysql

test-sqlite:
	@echo "==> Running SQLite integration tests (using $(CONTAINER_ENGINE))..."
	go test -v -tags=integration -timeout=5m ./drivers/sqlite

test-clickhouse:
	@echo "==> Running ClickHouse integration tests (using $(CONTAINER_ENGINE))..."
	go test -v -tags=integration -timeout=5m ./drivers/clickhouse

test-mssql:
	@echo "==> Running MSSQL integration tests (using $(CONTAINER_ENGINE))..."
	go test -v -tags=integration -timeout=5m ./drivers/mssql

test-cockroachdb:
	@echo "==> Running CockroachDB integration tests (using $(CONTAINER_ENGINE))..."
	go test -v -tags=integration -timeout=5m ./drivers/cockroachdb

test-all:
	@echo "==> Running all tests (unit + integration)..."
	@$(MAKE) test
	@$(MAKE) test-integration

coverage:
	@echo "==> Generating coverage report..."
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"

coverage-integration:
	@echo "==> Generating integration coverage report (using $(CONTAINER_ENGINE))..."
	go test -tags=integration -coverprofile=coverage-integration.out ./drivers/postgres ./drivers/mysql ./drivers/sqlite ./drivers/cockroachdb ./drivers/clickhouse ./drivers/mssql ./tests/integration
	go tool cover -html=coverage-integration.out -o coverage-integration.html
	@echo "Integration coverage report: coverage-integration.html"

install-deps:
	@echo "==> Installing testcontainers-go..."
	go get github.com/testcontainers/testcontainers-go@latest
	go mod tidy

tools:
	@echo "==> Installing development tools..."
	@which golangci-lint > /dev/null || (echo "Installing golangci-lint..." && \
		curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(shell go env GOPATH)/bin latest)
	@echo "Development tools installed"

lint:
	@echo "==> Running golangci-lint..."
	golangci-lint run --timeout=5m ./...

fmt:
	@echo "==> Checking code formatting..."
	@gofmt -l . | grep -v '^$$' && echo "Error: Files need formatting. Run 'gofmt -w .'" && exit 1 || echo "All files are formatted"

fmt-fix:
	@echo "==> Formatting code..."
	gofmt -w .
	@echo "Code formatted"

vet:
	@echo "==> Running go vet..."
	go vet ./...
	@echo "go vet passed"

bench:
	@echo "==> Running benchmarks..."
	go test -bench=. -benchmem ./...

ci-quick: fmt vet lint test
	@echo "Quick CI checks passed"

clean:
	@echo "==> Cleaning test artifacts..."
	rm -f coverage.out coverage.html
	rm -f coverage-integration.out coverage-integration.html
	rm -rf tests/integration/tmp

check-docker:
ifeq ($(CONTAINER_ENGINE),Podman)
	@podman info > /dev/null 2>&1 || (echo "Error: Podman is not running" && exit 1)
	@echo "Podman is running"
else
	@docker info > /dev/null 2>&1 || (echo "Error: Docker is not running" && exit 1)
	@echo "Docker is running"
endif

ci: fmt vet lint test check-docker test-integration
	@echo "All CI checks passed"
