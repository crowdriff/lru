package lru_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestLru(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Lru Suite")
}
