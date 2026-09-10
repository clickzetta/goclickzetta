NAME:=goclickzetta
VERSION:=$(shell git describe --tags --abbrev=0)
REVISION:=$(shell git rev-parse --short HEAD)
COVFLAGS:=
CURDIR := $(shell pwd)

## Run fmt, lint and test
all: fmt lint cov

include goclickzetta.mak

go: check
	$(CURDIR)/scripts/generate_go.sh


## Run the tests that need no server. The DSN is unset on purpose: the
## integration tests skip themselves without it, so this target means the same
## thing on a developer machine as it does in CI.
test:
	env -u CZ_TEST_DSN go test -race -count=1 $(COVFLAGS) ./...

## Run every test, including the ones that talk to a live instance. Needs
## CZ_TEST_DSN.
test-integration:
	@test -n "$$CZ_TEST_DSN" || { echo "CZ_TEST_DSN is not set" >&2; exit 1; }
	go test -race -count=1 -timeout 30m $(COVFLAGS) ./...

## Run Coverage tests
cov:
	$(MAKE) test COVFLAGS="-coverprofile=coverage.txt -covermode=atomic"

## Run Coverage over the integration tests as well
cov-integration:
	$(MAKE) test-integration COVFLAGS="-coverprofile=coverage.txt -covermode=atomic"



## Lint
lint: clint

## Format the driver, and any sample program under cmd/ if that directory
## exists. It does not in this repository, and an unguarded `ls cmd` failed the
## whole target.
fmt: cfmt
	@test -d cmd || exit 0; \
	for c in $$(ls cmd); do \
		(cd cmd/$$c;  $(MAKE) fmt); \
	done

## Install sample programs
install:
	for c in $$(ls cmd); do \
		(cd cmd/$$c;  GOBIN=$$GOPATH/bin go install $$c.go); \
	done

## Build fuzz tests
fuzz-build:
	for c in $$(ls | grep -E "fuzz-*"); do \
		(cd $$c; make fuzz-build); \
	done

## Run fuzz-dsn
fuzz-dsn:
	(cd fuzz-dsn; go-fuzz -bin=./dsn-fuzz.zip -workdir=.)

.PHONY: setup deps update test test-integration cov cov-integration lint help fuzz-dsn
