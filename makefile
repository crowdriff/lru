.PHONY: all

all:
	@echo "make <cmd>"
	@echo ""
	@echo "commands:"
	@ehco "  deps     - install dependencies"
	@echo "  test     - run all tests"

deps:
	go get github.com/boltdb/bolt
	go get github.com/golang/lint/golint
	go get github.com/onsi/ginkgo/ginkgo
	go get github.com/onsi/gomega

test:
	@go vet ./...
	@golint ./...
	@ginkgo -r -v -cover -race
