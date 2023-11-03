REPO_ROOT = $(shell pwd -P)
PACKAGES := $(shell go list ./... | grep -v examples)

GOLANGCILINT_VERSION = 1.54
GOTESTSUM_VERSION = 1.8.2
# make file to hold the logic of build and test setup
ZK_VERSION ?= 3.6.2

# Apache changed the name of the archive in version 3.5.x and seperated out src and binary packages
ZK_MINOR_VER=$(word 2, $(subst ., ,$(ZK_VERSION)))
ifeq ($(shell test $(ZK_MINOR_VER) -le 4; echo $$?),0)
  ZK = zookeeper-$(ZK_VERSION)
else
  ZK = apache-zookeeper-$(ZK_VERSION)-bin
endif
ZK_URL = "https://archive.apache.org/dist/zookeeper/zookeeper-$(ZK_VERSION)/$(ZK).tar.gz"

# Get the currently used golang install path (in GOPATH/bin, unless GOBIN is set)
ifeq (,$(shell go env GOBIN))
GOBIN=$(shell go env GOPATH)/bin
else
GOBIN=$(shell go env GOBIN)
endif

.DEFAULT_GOAL := test

validate-java:
	which java

$(ZK): validate-java
	wget $(ZK_URL)
	tar -zxf $(ZK).tar.gz
	rm $(ZK).tar.gz

zookeeper: $(ZK)
	# we link to a standard directory path so then the tests dont need to find based on version
	# in the test code. this allows backward compatible testing.
	ln -s $(ZK) zookeeper

.PHONY: setup
setup: zookeeper

.PHONY: lint
lint: golangci-lint
	go vet ./...
	$(GOLANGCILINT) run -v --deadline 10m

.PHONY: lint-fix
lint-fix: golangci-lint
	go fmt ./...
	go vet ./...
	$(GOLANGCILINT) run -v --deadline 10m --fix

.PHONY: build
build:
	go build ./...

.PHONY: test
test: gotestsum build zookeeper
	ZK_VERSION=$(ZK_VERSION) $(GOTESTSUM) --format dots -- -timeout 500s -v -race -covermode atomic -coverprofile profile.cov $(PACKAGES)

.PHONY: clean
clean:
	rm -f apache-zookeeper-*.tar.gz
	rm -f zookeeper-*.tar.gz
	rm -rf apache-zookeeper-*/
	rm -rf zookeeper-*/
	rm -f zookeeper
	rm -f profile.cov

LOCALBIN ?= $(shell pwd)/bin

$(LOCALBIN):
	mkdir -p $(LOCALBIN)

## Tool Binaries
GOLANGCILINT ?= $(LOCALBIN)/golangci-lint
GOTESTSUM ?= $(LOCALBIN)/gotestsum

.PHONY: golangci-lint
golangci-lint: $(GOLANGCILINT) ## Download golangci-lint locally if necessary.
$(GOLANGCILINT): $(LOCALBIN)
	test -s $(LOCALBIN)/golangci-lint || GOBIN=$(LOCALBIN) go install github.com/golangci/golangci-lint/cmd/golangci-lint@v${GOLANGCILINT_VERSION}

.PHONY: gotestsum
gotestsum: $(GOTESTSUM) ## Download gotestsum locally if necessary.
$(GOTESTSUM): $(LOCALBIN)
	test -s $(LOCALBIN)/gotestsum || GOBIN=$(LOCALBIN) go install gotest.tools/gotestsum@v${GOTESTSUM_VERSION}
