package lru

import (
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

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

	Context("Close", func() {

		It("should return an error when closing the remote store", func() {
			l := newDefaultLRU()
			defer closeBoltDB(l)
			l.store = &errStore{}
			err := l.Close()
			Ω(err).Should(HaveOccurred())
			Ω(err.Error()).Should(Equal("test error"))
		})

		It("should close the bolt database successfully", func() {
			l := newDefaultLRU()
			defer closeBoltDB(l)
			err := l.Close()
			Ω(err).ShouldNot(HaveOccurred())
		})
	})

	Context("Get", func() {

		It("should return a value from the local bolt cache", func() {
			l := newDefaultLRU()
			defer closeBoltDB(l)
			err := l.put([]byte("key"), []byte("value"))
			Ω(err).ShouldNot(HaveOccurred())
			l.store = &errStore{}
			val, err := l.Get([]byte("key"))
			Ω(err).ShouldNot(HaveOccurred())
			Ω(val).ShouldNot(BeNil())
			Ω(string(val)).Should(Equal("value"))
		})

		It("should return a value from the remote store", func() {
			l := newDefaultLRU()
			defer closeBoltDB(l)
			var reachedStore bool
			l.store = newStore(func(key []byte) ([]byte, error) {
				Ω(string(key)).Should(Equal("key"))
				reachedStore = true
				return []byte("value"), nil
			})
			val, err := l.Get([]byte("key"))
			Ω(err).ShouldNot(HaveOccurred())
			Ω(val).ShouldNot(BeNil())
			Ω(string(val)).Should(Equal("value"))
			Ω(reachedStore).Should(BeTrue())
		})

		It("should return an error if the remote store returns an error", func() {
			l := newDefaultLRU()
			defer closeBoltDB(l)
			_, err := l.Get([]byte("key"))
			Ω(err).Should(HaveOccurred())
			Ω(err).Should(MatchError(errNoStore))
		})
	})

	Context("hit", func() {

		It("should return false and increment misses when a cache miss occurs", func() {
			l := newDefaultLRU()
			defer closeBoltDB(l)
			ok := l.hit([]byte("key"))
			Ω(ok).Should(BeFalse())
			Ω(l.misses).Should(Equal(int64(1)))
			Ω(l.hits).Should(Equal(int64(0)))
			Ω(l.bget).Should(Equal(int64(0)))
		})

		It("should return true and increment hits/bget when a cache hit occurs", func() {
			l := newDefaultLRU()
			defer closeBoltDB(l)
			err := l.put([]byte("key"), []byte("value"))
			Ω(err).ShouldNot(HaveOccurred())
			ok := l.hit([]byte("key"))
			Ω(ok).Should(BeTrue())
			Ω(l.misses).Should(Equal(int64(0)))
			Ω(l.hits).Should(Equal(int64(1)))
			Ω(l.bget).Should(Equal(int64(5)))
		})
	})

	Context("Empty", func() {

		It("should empty the LRU and underlying bolt database", func() {
			l := newDefaultLRU()
			defer closeBoltDB(l)
			for i := 0; i < 4; i++ {
				err := l.put([]byte(strconv.Itoa(i)), []byte("value"))
				Ω(err).ShouldNot(HaveOccurred())
			}
			Ω(l.items).Should(HaveLen(4))
			err := l.Empty()
			Ω(err).ShouldNot(HaveOccurred())
			Ω(l.items).Should(HaveLen(0))
			Ω(l.list.Len()).Should(Equal(0))
			Ω(l.remain).Should(Equal(l.cap))
			for i := 0; i < 4; i++ {
				val := l.getFromBolt([]byte(strconv.Itoa(i)))
				Ω(val).Should(BeNil())
			}
		})
	})

	Context("getFromStore", func() {

		It("should return an error when the remote store returns an error", func() {
			l := newDefaultLRU()
			defer closeBoltDB(l)
			l.store = &errStore{}
			v, err := l.getFromStore([]byte("key"))
			Ω(err).Should(HaveOccurred())
			Ω(err.Error()).Should(Equal("test error"))
			Ω(v).Should(BeNil())
		})

		It("should return the value from the remote store", func() {
			l := newDefaultLRU()
			defer closeBoltDB(l)
			l.store = newStore(func(key []byte) ([]byte, error) {
				Ω(string(key)).Should(Equal("key"))
				return []byte("value"), nil
			})
			v, err := l.getFromStore([]byte("key"))
			Ω(err).ShouldNot(HaveOccurred())
			Ω(v).ShouldNot(BeNil())
			Ω(string(v)).Should(Equal("value"))
			Eventually(func() bool {
				v, err := l.Get([]byte("key"))
				return err == nil && v != nil && string(v) == "value"
			}, 100*time.Millisecond, time.Millisecond).Should(BeTrue())
		})

		It("shouldn't make mulitple requests to the remote store for the same key", func() {
			l := newDefaultLRU()
			defer closeBoltDB(l)
			var reqs int64
			l.store = newStore(func(key []byte) ([]byte, error) {
				atomic.AddInt64(&reqs, 1)
				time.Sleep(time.Millisecond)
				return []byte("value"), nil
			})
			var hookCalls int64
			l.PostStoreFn = func(val []byte, err error) ([]byte, error) {
				atomic.AddInt64(&hookCalls, 1)
				Ω(err).ShouldNot(HaveOccurred())
				Ω(string(val)).Should(Equal("value"))
				return []byte("new value"), nil
			}
			var wg sync.WaitGroup
			for i := 0; i < 3; i++ {
				wg.Add(1)
				go func() {
					defer GinkgoRecover()
					v, err := l.getFromStore([]byte("key"))
					Ω(err).ShouldNot(HaveOccurred())
					Ω(v).ShouldNot(BeNil())
					Ω(string(v)).Should(Equal("new value"))
					wg.Done()
				}()
			}
			wg.Wait()
			Ω(reqs).Should(Equal(int64(1)))
			Ω(hookCalls).Should(Equal(int64(1)))
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

	Context("put", func() {

		It("should insert a value into the bolt database and the LRU cache", func() {
			l := newDefaultLRU()
			defer closeBoltDB(l)
			err := l.put([]byte("key"), []byte("value"))
			Ω(err).ShouldNot(HaveOccurred())
			exists := l.hit([]byte("key"))
			Ω(exists).Should(BeTrue())
			v := l.getFromBolt([]byte("key"))
			Ω(v).ShouldNot(BeNil())
			Ω(string(v)).Should(Equal("value"))
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
