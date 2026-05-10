.PHONY: build test test-race test-integration lint clean plugin-%

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

# Build a specific plugin as .so for dynamic loading.
# Usage: make plugin-influxdb  ->  plugins/influxdb.so
# Uses the wrapper in cmd/plugin-<name>/main.go (package main required for -buildmode=plugin).
plugin-%:
	@mkdir -p plugins
	go build -tags=plugin -buildmode=plugin -o plugins/$*.so ./cmd/plugin-$*

clean:
	rm -rf bin/ plugins/*.so
