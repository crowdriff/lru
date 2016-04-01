.PHONY: all

all:
	@echo "make <cmd>"
	@echo ""
	@echo "commands:"
	@echo "  benchmark    - run all benchmarks"
	@echo "  deps        - install dependencies from Glockfile"
	@echo "  test        - run all tests"
	@echo "  tools       - install dev dependencies"
	@echo "  update_deps - update Glockfile"

benchmark:
	@go test -bench=. -benchmem

deps:
	@glock sync -n github.com/crowdriff/lru < Glockfile

test:
	@go vet ./...
	@golint ./...
	@ginkgo -r -v -cover -race

tools:
	go get -u github.com/golang/lint/golint
	go get -u github.com/robfig/glock
	go get -u github.com/onsi/ginkgo/ginkgo
	go get -u github.com/onsi/gomega

update_deps:
	@glock save -n github.com/crowdriff/lru > Glockfile
