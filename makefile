.PHONY: all

all:
	@echo "make <cmd>"
	@echo ""
	@echo "commands:"
	@echo "  deps        - install dependencies from Glockfile"
	@echo "  test        - run all tests"
	@echo "  tools       - install dev dependencies"
	@echo "  update_deps - update Glockfile"

deps:
	@glock sync -n github.com/crowdriff/lru < Glockfile

test:
	@go vet ./...
	@golint ./...
	@ginkgo -r -v -cover -race

tools:
	go get github.com/golang/lint/golint
	go get github.com/robfig/glock
	go get github.com/onsi/ginkgo/ginkgo
	go get github.com/onsi/gomega

update_deps:
	@glock save -n github.com/crowdriff/lru > Glockfile
