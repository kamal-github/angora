.PHONY: all

all: test test-dep-up test-cov

test-dep-up:
	docker-compose up -d

test-dep-down:
	docker-compose down --remove-orphans

test: test-dep-up
	go test -v -race ./...

test-cov: test-dep-up
	go test -v -race -coverprofile angoracov.out ./...
	go tool cover -html=angoracov.out
