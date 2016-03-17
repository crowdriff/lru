package lru

import (
	"io"
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
	defer closeBoltDB(l)
	err := l.Open()
	Ω(err).ShouldNot(HaveOccurred())
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
	if l.db != nil {
		l.Close()
	}
}

func stringFromWriterTo(wt io.WriterTo) string {
	buf := getBuf()
	defer putBuf(buf)
	_, err := wt.WriteTo(buf)
	Ω(err).ShouldNot(HaveOccurred())
	return buf.String()
}

func newStore(get func([]byte) ([]byte, error)) Store {
	return &testStore{get}
}

type testStore struct {
	get func([]byte) ([]byte, error)
}

func (s *testStore) Open() error {
	return nil
}
func (s *testStore) Close() error {
	return nil
}
func (s *testStore) Get(key []byte) ([]byte, error) {
	return s.get(key)
}
