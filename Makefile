.PHONY: all build test race cover lint vet fmt run clean

BINARY := bin/gateway

all: fmt vet test build

build:
	go build -trimpath -ldflags="-s -w" -o $(BINARY) ./cmd/gateway

test:
	go test ./... -count=1

race:
	go test ./... -race -count=1

cover:
	go test ./... -race -count=1 -coverpkg=./internal/... -coverprofile=coverage.out
	go tool cover -func=coverage.out | tail -1

vet:
	go vet ./...

fmt:
	gofmt -l -w .

run:
	go run ./cmd/gateway

clean:
	rm -rf bin coverage.out
