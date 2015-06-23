clean:
	go clean ./...

doc:
	godoc -http=:6060

build-generators:
	go build -o $(GOPATH)/bin/origins-generate-pedsnet-vocab \
		./generators/vocab

build: build-generators

install:
	go get golang.org/x/tools/cmd/cover
	go get github.com/cespare/prettybench

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
