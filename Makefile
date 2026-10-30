.PHONY: build test test-short race e2e lint vet fmt bench loadgen docker clean

BIN := bin/ingestor

build:
	go build -trimpath -o $(BIN) ./cmd/ingestor

test:
	go test -count=1 ./...

# Skips the end-to-end tests that build and crash the real binary.
test-short:
	go test -short -count=1 ./...

race:
	go test -race -count=1 ./...

e2e:
	go test -count=1 -v ./internal/e2e/

fmt:
	gofmt -w .

vet:
	go vet ./...

lint:
	golangci-lint run

bench:
	go test -run XXX -bench . -benchmem ./internal/wal/

loadgen:
	go build -o bin/loadgen ./tools/loadgen

docker:
	docker build -t wal-ingestion-engine .

clean:
	rm -rf bin data ingestions processing failed
