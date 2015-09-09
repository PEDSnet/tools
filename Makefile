GIT_VERSION := $(shell git log -1 --pretty=format:"%h (%ci)")

clean:
	go clean ./...

doc:
	godoc -http=:6060

build-generators:
	go build -o $(GOPATH)/bin/origins-generate-pedsnet-vocab \
		./generators/vocab
	go build -o $(GOPATH)/bin/origins-generate-pedsnet-etl \
		./generators/etl
	go build -o $(GOPATH)/bin/origins-generate-pedsnet-dqa \
		./generators/dqa

build-services:
	cd ./services/dqa && go build \
		-ldflags "-X main.buildVersion '$(GIT_VERSION)'" \
		-o $(GOPATH)/bin/pedsnet-dqa

build: build-generators build-services

install:
	go get golang.org/x/tools/cmd/cover
	go get github.com/cespare/prettybench
	go get github.com/spf13/viper
	go get github.com/spf13/cobra

test:
	go test -cover ./...

bench:
	go test -run=none -bench=. ./... | prettybench

fmt:
	go vet ./...
	go fmt ./...

lint:
	golint ./...

.PHONY: test
