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

	Context("NewLRU", func() {

		It("should return an LRU with the default values set", func() {
			l := NewLRU("", "", DefaultTwoQ(0), nil)
			defer closeBoltDB(l)
			Ω(l.lru.Cap()).Should(Equal(int64(1000)))
			Ω(l.lru.Size()).Should(Equal(int64(0)))
			Ω(l.dbPath).Should(Equal("/tmp/lru.db"))
			Ω(string(l.bName)).Should(Equal("lru"))
			Ω(l.store).ShouldNot(BeNil())
			Ω(l.reqs).ShouldNot(BeNil())
			Ω(l.lru).ShouldNot(BeNil())
		})

		It("should return an LRU with the custom values set", func() {
			s := &errStore{}
			l := NewLRU("dbPath", "bName", DefaultTwoQ(10e6), s)
			defer closeBoltDB(l)
			Ω(l.lru.Cap()).Should(Equal(int64(10e6)))
			Ω(l.lru.Len()).Should(Equal(int64(0)))
			Ω(l.lru.Size()).Should(Equal(int64(0)))
			Ω(l.dbPath).Should(Equal("dbPath"))
			Ω(string(l.bName)).Should(Equal("bName"))
			Ω(l.store).Should(Equal(s))
			Ω(l.reqs).ShouldNot(BeNil())
			Ω(l.lru).ShouldNot(BeNil())
		})
	})

	Context("Open", func() {

		It("should return an error when opening", func() {
			l := NewLRU("", "", nil, &errStore{})
			defer closeBoltDB(l)
			err := l.Open()
			Ω(err).Should(HaveOccurred())
			Ω(err.Error()).Should(Equal("test error"))
		})

		It("should open the bolt database successfully", func() {
			l := NewLRU("", "", nil, nil)
			defer closeBoltDB(l)
			err := l.Open()
			Ω(err).ShouldNot(HaveOccurred())
			Ω(l.db).ShouldNot(BeNil())
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

		It("should return an error when no key is provided", func() {
			l := newDefaultLRU()
			defer closeBoltDB(l)
			b, err := l.Get(nil)
			Ω(b).Should(BeNil())
			Ω(err).Should(HaveOccurred())
			Ω(err).Should(MatchError(ErrNoKey))
		})

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

		It("should return an error from the remote store if it hits the LRU but isn't found in the database", func() {
			l := newDefaultLRU()
			defer closeBoltDB(l)
			l.lru.PutAndEvict([]byte("key"), 400)
			_, err := l.Get([]byte("key"))
			Ω(err).Should(HaveOccurred())
			Ω(err.Error()).Should(Equal("no remote store available"))
			Ω(l.hits).Should(Equal(int64(0)))
			Ω(l.bget).Should(Equal(int64(0)))
			Ω(l.misses).Should(Equal(int64(1)))
		})
	})

	Context("GetWriterTo", func() {

		It("should return an error when no key is provided", func() {
			l := newDefaultLRU()
			defer closeBoltDB(l)
			wt, err := l.GetWriterTo(nil)
			Ω(wt).Should(BeNil())
			Ω(err).Should(HaveOccurred())
			Ω(err).Should(MatchError(ErrNoKey))
		})

		It("should return a value from the local bolt cache", func() {
			l := newDefaultLRU()
			defer closeBoltDB(l)
			err := l.put([]byte("key"), []byte("value"))
			Ω(err).ShouldNot(HaveOccurred())
			l.store = &errStore{}
			wt, err := l.GetWriterTo([]byte("key"))
			Ω(err).ShouldNot(HaveOccurred())
			Ω(wt).ShouldNot(BeNil())
			val := stringFromWriterTo(wt)
			Ω(val).Should(Equal("value"))
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
			wt, err := l.GetWriterTo([]byte("key"))
			Ω(err).ShouldNot(HaveOccurred())
			Ω(wt).ShouldNot(BeNil())
			val := stringFromWriterTo(wt)
			Ω(val).Should(Equal("value"))
			Ω(reachedStore).Should(BeTrue())
		})

		It("should return an error if the remote store returns an error", func() {
			l := newDefaultLRU()
			defer closeBoltDB(l)
			wt, err := l.GetWriterTo([]byte("key"))
			Ω(err).Should(HaveOccurred())
			Ω(err).Should(MatchError(errNoStore))
			Ω(wt).Should(BeNil())
		})

		It("should return an error from the remote store if it hits the LRU but isn't found in the database", func() {
			l := newDefaultLRU()
			defer closeBoltDB(l)
			l.lru.PutAndEvict([]byte("key"), 400)
			_, err := l.GetWriterTo([]byte("key"))
			Ω(err).Should(HaveOccurred())
			Ω(err.Error()).Should(Equal("no remote store available"))
			Ω(l.hits).Should(Equal(int64(0)))
			Ω(l.bget).Should(Equal(int64(0)))
			Ω(l.misses).Should(Equal(int64(1)))
		})
	})

	Context("hit", func() {

		It("should return false and increment misses when a cache miss occurs", func() {
			l := newDefaultLRU()
			defer closeBoltDB(l)
			size := l.hit([]byte("key"))
			Ω(size).Should(BeNumerically("<", 0))
			Ω(l.misses).Should(Equal(int64(1)))
			Ω(l.hits).Should(Equal(int64(0)))
			Ω(l.bget).Should(Equal(int64(0)))
		})

		It("should return true and increment hits/bget when a cache hit occurs", func() {
			l := newDefaultLRU()
			defer closeBoltDB(l)
			err := l.put([]byte("key"), []byte("value"))
			Ω(err).ShouldNot(HaveOccurred())
			size := l.hit([]byte("key"))
			Ω(size).Should(Equal(int64(5)))
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
			Ω(l.lru.Len()).Should(Equal(int64(4)))
			err := l.Empty()
			Ω(err).ShouldNot(HaveOccurred())
			Ω(l.lru.Len()).Should(Equal(int64(0)))
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
			var wg sync.WaitGroup
			for i := 0; i < 3; i++ {
				wg.Add(1)
				go func() {
					defer GinkgoRecover()
					v, err := l.getFromStore([]byte("key"))
					Ω(err).ShouldNot(HaveOccurred())
					Ω(v).ShouldNot(BeNil())
					Ω(string(v)).Should(Equal("value"))
					wg.Done()
				}()
			}
			wg.Wait()
			Ω(reqs).Should(Equal(int64(1)))
		})

		It("should return an error when the store returns a nil value and error", func() {
			l := newDefaultLRU()
			defer closeBoltDB(l)
			l.store = newStore(func(key []byte) ([]byte, error) {
				return nil, nil
			})
			val, err := l.getFromStore([]byte("key"))
			Ω(err).Should(HaveOccurred())
			Ω(err).Should(MatchError(ErrNoValue))
			Ω(val).Should(BeNil())
		})

		It("should recover from a panic in the store's Get and return an error", func() {
			l := newDefaultLRU()
			defer closeBoltDB(l)
			l.store = newStore(func(key []byte) ([]byte, error) {
				panic("error message")
			})
			val, err := l.getFromStore([]byte("key"))
			Ω(err).Should(HaveOccurred())
			Ω(err.Error()).Should(Equal("panic: error message"))
			Ω(val).Should(BeNil())
		})
	})

	Context("put", func() {

		It("should insert a value into the bolt database and the LRU cache", func() {
			l := newDefaultLRU()
			defer closeBoltDB(l)
			err := l.put([]byte("key"), []byte("value"))
			Ω(err).ShouldNot(HaveOccurred())
			size := l.hit([]byte("key"))
			Ω(size).Should(Equal(int64(5)))
			v := l.getFromBolt([]byte("key"))
			Ω(v).ShouldNot(BeNil())
			Ω(string(v)).Should(Equal("value"))
		})
	})

	Context("addItem", func() {

		It("should prune values from the bolt database when capacity is exceeded", func() {
			l := newDefaultLRU()
			defer closeBoltDB(l)
			for i := 0; i < 3; i++ {
				err := l.put([]byte(strconv.Itoa(i)), make([]byte, 240))
				Ω(err).ShouldNot(HaveOccurred())
			}

			l.put([]byte("3"), make([]byte, 300))
			Ω(l.puts).Should(Equal(int64(4)))
			Ω(l.bput).Should(Equal(int64(1020)))
			Ω(l.lru.Len()).Should(Equal(int64(3)))
			v := l.getFromBolt([]byte("0"))
			Ω(v).Should(BeNil())
			for i := 1; i < 4; i++ {
				v := l.getFromBolt([]byte(strconv.Itoa(i)))
				Ω(v).ShouldNot(BeNil())
			}
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
