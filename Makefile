.PHONY: build test test-race test-integration lint clean

build:
	go build -o bin/databridge ./cmd/databridge

test:
	go test ./... -count=1 -v

test-race:
	go test -race ./... -count=1

test-integration:
	go test -tags=integration -v -count=1 -timeout 10m ./testing/integration/

lint:
	golangci-lint run ./...

clean:
	rm -rf bin/
