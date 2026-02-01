.PHONY: all build test clean docker install lint fmt help

# Binary name
BINARY=state-actor
VERSION?=$(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
LDFLAGS=-ldflags "-X main.Version=$(VERSION)"

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOTEST=$(GOCMD) test
GOCLEAN=$(GOCMD) clean
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod
GOFMT=$(GOCMD) fmt

# Default target
all: build

## build: Build the binary
build:
	$(GOBUILD) $(LDFLAGS) -o $(BINARY) .

## install: Install to $GOPATH/bin
install:
	$(GOCMD) install $(LDFLAGS) .

## test: Run tests
test:
	$(GOTEST) -v ./...

## test-race: Run tests with race detector
test-race:
	$(GOTEST) -race -v ./...

## test-coverage: Run tests with coverage
test-coverage:
	$(GOTEST) -coverprofile=coverage.out ./...
	$(GOCMD) tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"

## bench: Run benchmarks
bench:
	$(GOTEST) -bench=. -benchmem ./generator

## lint: Run linter
lint:
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run; \
	else \
		echo "golangci-lint not installed, running go vet instead"; \
		$(GOCMD) vet ./...; \
	fi

## fmt: Format code
fmt:
	$(GOFMT) ./...

## clean: Clean build artifacts
clean:
	$(GOCLEAN)
	rm -f $(BINARY)
	rm -f coverage.out coverage.html
	rm -rf dist/

## docker: Build Docker image
docker:
	docker build -t state-actor:latest .
	docker build -t state-actor:$(VERSION) .

## tidy: Tidy go modules
tidy:
	$(GOMOD) tidy

## deps: Download dependencies
deps:
	$(GOMOD) download

## example: Run example generation
example:
	./$(BINARY) \
		--db /tmp/example-chaindata \
		--genesis examples/test-genesis.json \
		--accounts 1000 \
		--contracts 500 \
		--max-slots 100 \
		--seed 42 \
		--verbose \
		--benchmark
	@echo ""
	@echo "Example database created at /tmp/example-chaindata"
	@du -sh /tmp/example-chaindata

## help: Show this help
help:
	@echo "State Actor - Ethereum State Generator"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/## /  /'
