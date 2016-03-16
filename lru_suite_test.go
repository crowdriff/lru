package lru

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestLru(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Lru Suite")
}

var _ = BeforeEach(func() {
	l := NewLRU(0, "", "", nil)
	err := l.Open()
	Ω(err).ShouldNot(HaveOccurred())
	defer l.Close()
	err = l.emptyBolt()
	Ω(err).ShouldNot(HaveOccurred())
})

func newDefaultLRU() *LRU {
	l := NewLRU(0, "", "", nil)
	err := l.Open()
	Ω(err).ShouldNot(HaveOccurred())
	return l
}

func closeBoltDB(l *LRU) {
	if l.cache != nil {
		l.Close()
	}
}
