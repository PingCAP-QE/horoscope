GOARCH := $(if $(GOARCH),$(GOARCH),amd64)
GO=GO15VENDOREXPERIMENT="1" CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) GO111MODULE=on go
GOTEST=GO15VENDOREXPERIMENT="1" CGO_ENABLED=1 GO111MODULE=on go test # go race detector requires cgo
VERSION   := $(if $(VERSION),$(VERSION),latest)
PACKAGE_LIST  := go list ./...| grep -vE "cmd"
PACKAGES  := $$($(PACKAGE_LIST))

GOBUILD=$(GO) build

default: build

build:
	$(GOBUILD) $(GOMOD) -o bin/horo cmd/*.go

fmt:
	go fmt ./...

test:
	go test ./...

vet:
	$(GO) vet -all $(PACKAGES)

tidy:
	@echo "go mod tidy"
	GO111MODULE=on go mod tidy
	@git diff --exit-code -- go.mod