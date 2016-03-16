package lru

import (
	"errors"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Lru", func() {

	Context("NetLRU", func() {

		It("should return an LRU with the default values set", func() {
			l := NewLRU(0, "", "", nil)
			defer closeBoltDB(l)
			Ω(int(l.cap)).Should(Equal(1000))
			Ω(int(l.remain)).Should(Equal(1000))
			Ω(l.dbPath).Should(Equal("/tmp/lru.db"))
			Ω(string(l.bName)).Should(Equal("lru"))
			Ω(l.store).ShouldNot(BeNil())
			Ω(l.reqs).ShouldNot(BeNil())
			Ω(l.list).ShouldNot(BeNil())
			Ω(l.items).ShouldNot(BeNil())
		})

		It("should return an LRU with the custom values set", func() {
			s := &errStore{}
			l := NewLRU(10e6, "dbPath", "bName", s)
			defer closeBoltDB(l)
			Ω(l.cap).Should(Equal(int64(10e6)))
			Ω(l.remain).Should(Equal(int64(10e6)))
			Ω(l.dbPath).Should(Equal("dbPath"))
			Ω(string(l.bName)).Should(Equal("bName"))
			Ω(l.store).Should(Equal(s))
			Ω(l.reqs).ShouldNot(BeNil())
			Ω(l.list).ShouldNot(BeNil())
			Ω(l.items).ShouldNot(BeNil())
		})
	})

	Context("Open", func() {

		It("should return an error when opening", func() {
			l := NewLRU(0, "", "", &errStore{})
			defer closeBoltDB(l)
			err := l.Open()
			Ω(err).Should(HaveOccurred())
			Ω(err.Error()).Should(Equal("test error"))
		})

		It("should open the bolt database successfully", func() {
			l := NewLRU(0, "", "", nil)
			defer closeBoltDB(l)
			err := l.Open()
			Ω(err).ShouldNot(HaveOccurred())
			Ω(l.cache).ShouldNot(BeNil())
		})
	})
})

type errStore struct{}

func (s *errStore) Open() error {
	return errors.New("test error")
}
func (s *errStore) Close() error {
	return errors.New("test error")
}
func (s *errStore) Get(k []byte) ([]byte, error) {
	return nil, errors.New("test error")
}
