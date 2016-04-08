PROG_NAME := "pedsnet-dqa"
GIT_VERSION := $(shell git log -1 --pretty=format:"%h (%ci)" .)

build:
	go build -ldflags "-X \"main.buildVersion=$(GIT_VERSION)\"" \
		-o $(GOPATH)/bin/$(PROG_NAME)

dist-build:
	mkdir -p dist

	# Enable CGO for linking to the SQLite package.
	gox -output="./dist/{{.OS}}-{{.Arch}}/$(PROG_NAME)" \
		-cgo -ldflags "-X \"main.buildVersion=$(GIT_VERSION)\"" \
		-os "darwin" \
		-arch "amd64" > /dev/null

dist-zip:
	cd dist && zip pedsnet-dqa-darwin-amd64.zip darwin-amd64/*

dist: dist-build dist-zip

install:
	go get github.com/mitchellh/gox
	go get golang.org/x/tools/cmd/cover
	go get github.com/spf13/viper
	go get github.com/mattn/go-sqlite3
	go get github.com/spf13/cobra
	go get github.com/chop-dbhi/data-models-service/client
	go get github.com/blang/semver
	go get github.com/olekukonko/tablewriter

test:
	go test -cover ./...

.PHONY: test build dist-build dist
