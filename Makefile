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

build-commands:
	mkdir -p bin

	go build -ldflags "-X \"main.buildVersion=$(GIT_VERSION)\"" \
		-o ./bin/pedsnet-dqa \
		./cmd/dqa

	go build -ldflags "-X \"main.buildVersion=$(GIT_VERSION)\"" \
		-o ./bin/pedsnet-etlprov \
		./cmd/etlprov

build-commands-dist:
	mkdir -p dist

	gox -ldflags "-X \"main.buildVersion=$(GIT_VERSION)\"" \
		-os "linux windows darwin" \
		-arch "amd64" \
		-output="./dist/pedsnet-etlprov-{{.OS}}-{{.Arch}}" \
		./cmd/etlprov

install:
	go get golang.org/x/tools/cmd/cover
	go get github.com/cespare/prettybench
	go get github.com/spf13/viper
	go get github.com/spf13/cobra
	go get github.com/mitchellh/gox
	go get github.com/chop-dbhi/data-models-service/client

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
