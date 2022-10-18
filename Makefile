# Variables
BUILD_DIR 		:= build
GITHASH 		:= $(shell git rev-parse HEAD)
VERSION			:= $(shell git describe --abbrev=0 --tags --always)
DATE			:= $(shell TZ=UTC date -u '+%Y-%m-%dT%H:%M:%SZ UTC')
LINT_PATHS		:= ./...
FORMAT_PATHS 	:= .

# Compilation variables
CC 					:= go build
DFLAGS 				:= -race
CFLAGS 				:= -X 'main.githash=$(GITHASH)' \
            -X 'main.date=$(DATE)' \
            -X 'main.version=$(VERSION)'
PLATFORMS=darwin linux windows
ARCHITECTURES=386 amd64

# Makefile variables
VPATH 				:= $(BUILD_DIR)

.SECONDEXPANSION:
.PHONY: all
all: init format lint test release

.PHONY: init
init:
	curl https://raw.githubusercontent.com/golang/dep/master/install.sh | sh
	go install github.com/onsi/ginkgo/v2/ginkgo@latest
	go get golang.org/x/tools/cmd/cover
	go get github.com/modocache/gover
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s v1.50.0
	go install golang.org/x/tools/cmd/goimports@latest

.PHONY: cleanmake
clean:
	rm -rf $(BUILD_DIR)
	rm -rf dist

.PHONY: format
format:
	gofmt -w -s $(FORMAT_PATHS)
	goimports -w $(FORMAT_PATHS)

.PHONY: lint
lint:
	@command -v ./bin/golangci-lint >/dev/null 2>&1 || { echo >&2 "golangci-lint is required but not available please follow instructions from https://github.com/golangci/golangci-lint"; exit 1; }
	./bin/golangci-lint run  --config golangci.yml

.PHONY: test
test:
	ginkgo -r --randomize-all --randomize-suites --fail-on-pending --cover --trace --progress --compilers=2

.PHONY: testrun
testrun:
	ginkgo watch -r ./

.PHONY: cover
cover:
	gover . coverage.txt


.PHONY: dev
dev: format lint build

.PHONY: build
build:
	$(CC) $(DFLAGS) -ldflags "-s -w $(CFLAGS)" -o $(BUILD_DIR)/ovh-spark-submit

.PHONY: release
release:
	$(CC) -ldflags "-s -w $(CFLAGS)" -o $(BUILD_DIR)/ovh-spark-submit

.PHONY: dist
dist:
		@for GOOS in $(PLATFORMS) ; do \
            for GOARCH in $(ARCHITECTURES) ; do \
                GOOS=$${GOOS} GOARCH=$${GOARCH} $(CC) -ldflags "-s -w $(CFLAGS)" -o $(BUILD_DIR)/ovh-spark-submit_$${GOOS}_$${GOARCH}; \
			done \
		done

.PHONY: install
install: release
	cp -v $(BUILD_DIR)/ovh-spark-submit $(GOPATH)/bin/ovh-spark-submit
