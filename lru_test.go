package lru

import (
	"errors"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("LRU", func() {

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

	Context("PreStoreFn", func() {

		It("should return an error when the PreStoreFn returns an error", func() {
			// set up the LRU
			l := newDefaultLRU()
			defer closeBoltDB(l)
			Ω(l.PreStoreFn).Should(BeNil())
			l.PreStoreFn = func(key []byte) ([]byte, error) {
				Ω(string(key)).Should(Equal("key"))
				return nil, errors.New("prestorefn error")
			}
			l.store = &errStore{}
			// make request
			v, err := l.getFromStore([]byte("key"))
			Ω(err).Should(HaveOccurred())
			Ω(err.Error()).Should(Equal("prestorefn error"))
			Ω(v).Should(BeNil())
		})

		It("should update the key provided to the store in the PreStoreFn", func() {
			// set up the LRU
			l := newDefaultLRU()
			defer closeBoltDB(l)
			Ω(l.PreStoreFn).Should(BeNil())
			l.PreStoreFn = func(key []byte) ([]byte, error) {
				Ω(string(key)).Should(Equal("key"))
				return []byte("newKey"), nil
			}
			l.store = newStore(func(key []byte) ([]byte, error) {
				Ω(string(key)).Should(Equal("newKey"))
				return nil, errors.New("store error")
			})
			// make request
			v, err := l.getFromStore([]byte("key"))
			Ω(err).Should(HaveOccurred())
			Ω(err.Error()).Should(Equal("store error"))
			Ω(v).Should(BeNil())
		})
	})

	Context("PostStoreFn", func() {

		It("should receive an error and respond with its own error", func() {
			// set up the LRU
			l := newDefaultLRU()
			defer closeBoltDB(l)
			Ω(l.PostStoreFn).Should(BeNil())
			l.PostStoreFn = func(val []byte, err error) ([]byte, error) {
				Ω(val).Should(BeNil())
				Ω(err).Should(HaveOccurred())
				Ω(err.Error()).Should(Equal("store error"))
				return nil, errors.New("poststorefn error")
			}
			l.store = newStore(func(key []byte) ([]byte, error) {
				Ω(string(key)).Should(Equal("key"))
				return nil, errors.New("store error")
			})
			// make request
			v, err := l.getFromStore([]byte("key"))
			Ω(err).Should(HaveOccurred())
			Ω(err.Error()).Should(Equal("poststorefn error"))
			Ω(v).Should(BeNil())
		})

		It("should receive a value and respond with its own value", func() {
			// set up the LRU
			l := newDefaultLRU()
			defer closeBoltDB(l)
			Ω(l.PostStoreFn).Should(BeNil())
			l.PostStoreFn = func(val []byte, err error) ([]byte, error) {
				Ω(val).ShouldNot(BeNil())
				Ω(string(val)).Should(Equal("store val"))
				Ω(err).ShouldNot(HaveOccurred())
				return []byte("new val"), nil
			}
			l.store = newStore(func(key []byte) ([]byte, error) {
				Ω(string(key)).Should(Equal("key"))
				return []byte("store val"), nil
			})
			// make request
			v, err := l.getFromStore([]byte("key"))
			Ω(err).ShouldNot(HaveOccurred())
			Ω(v).ShouldNot(BeNil())
			Ω(string(v)).Should(Equal("new val"))
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
