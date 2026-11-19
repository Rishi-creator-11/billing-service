SHELL := /bin/bash
export PATH := $(HOME)/go/bin:$(PATH)

API_KEY ?= dev-local-key
GRPC_ADDR ?= localhost:50051
SPANNER_EMULATOR_HOST ?= localhost:9010

.PHONY: help up down logs proto proto-lint proto-breaking lint vet test bench build demo load emulator

help: ## list targets
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-16s %s\n", $$1, $$2}'

up: ## build and start the full stack (Spanner emulator, service, Prometheus, Grafana, Jaeger)
	docker compose up -d --build --wait billing prometheus grafana jaeger
	@echo
	@echo "gRPC            localhost:50051   (api key: $(API_KEY))"
	@echo "REST gateway    http://localhost:8080"
	@echo "service metrics http://localhost:9091/metrics"
	@echo "Prometheus      http://localhost:9090"
	@echo "Grafana         http://localhost:3000   (dashboard: Billing service)"
	@echo "Jaeger          http://localhost:16686"

down: ## stop the stack and delete its data
	docker compose down -v

logs: ## follow service logs
	docker compose logs -f billing

emulator: ## start only the Spanner emulator (for `make test`)
	docker compose up -d spanner

proto: ## regenerate code from proto/ with buf
	buf generate

proto-lint: ## lint the protobuf contract
	buf lint

proto-breaking: ## check for breaking API changes against main
	buf breaking --against '.git#branch=main,subdir=proto' || true

lint: proto-lint ## buf lint, go vet and golangci-lint
	go vet ./...
	golangci-lint run ./...

vet:
	go vet ./...

build: ## build binaries into ./bin
	go build -o bin/ ./cmd/...

test: ## all tests with -race; Spanner tests run when the emulator is up on $(SPANNER_EMULATOR_HOST)
	@if nc -z $(subst :, ,$(SPANNER_EMULATOR_HOST)) 2>/dev/null; then \
	  echo "Spanner emulator detected: running store tests against it"; \
	  SPANNER_EMULATOR_HOST=$(SPANNER_EMULATOR_HOST) go test -race -count=1 ./...; \
	else \
	  echo "No emulator on $(SPANNER_EMULATOR_HOST): Spanner tests are skipped (run 'make emulator')"; \
	  go test -race -count=1 ./...; \
	fi

bench: ## service-layer benchmarks on the in-memory store
	go test -run '^$$' -bench . -benchmem ./internal/service

demo: ## walk through the API against the running stack
	go run ./cmd/demo -addr $(GRPC_ADDR) -api-key $(API_KEY)

load: ## load test against the running stack (override with ARGS="-c 32 -d 20s")
	go run ./cmd/loadtest -addr $(GRPC_ADDR) -api-key $(API_KEY) $(ARGS)
