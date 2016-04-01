package lru

import (
	"strconv"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Basiclru", func() {

	Context("NewBasicLRU", func() {

		It("should return a new BasicLRU with the default eviction ratio", func() {
			l := DefaultBasicLRU(0)
			Ω(l).ShouldNot(BeNil())
			Ω(l.Cap()).Should(Equal(int64(1000)))
			Ω(l.Len()).Should(Equal(int64(0)))
			Ω(l.Size()).Should(Equal(int64(0)))
			Ω(l.pruneCap).Should(Equal(int64(999)))
		})

		It("should return a new BasicLRU with a maximum prune capacity of its capacity", func() {
			l := NewBasicLRU(0, -1.0)
			Ω(l).ShouldNot(BeNil())
			Ω(l.cap).Should(Equal(int64(1000)))
			Ω(l.Len()).Should(Equal(int64(0)))
			Ω(l.size).Should(Equal(int64(0)))
			Ω(l.pruneCap).Should(Equal(l.cap))
		})

		It("should return a new BasicLRU with a mimimum prune capacity of 0", func() {
			l := NewBasicLRU(0, 2.0)
			Ω(l).ShouldNot(BeNil())
			Ω(l.cap).Should(Equal(int64(1000)))
			Ω(l.Len()).Should(Equal(int64(0)))
			Ω(l.size).Should(Equal(int64(0)))
			Ω(l.pruneCap).Should(Equal(int64(0)))
		})
	})

	Context("Get", func() {

		It("should return -1 when the key doesn't exist in the LRU", func() {
			l := DefaultBasicLRU(0)
			size := l.Get([]byte("nokey"))
			Ω(size).Should(Equal(int64(-1)))
		})

		It("should return the size when the key does exist in the LRU", func() {
			l := DefaultBasicLRU(0)
			i := &lruItem{key: []byte("good"), size: 100}
			i.elem = l.list.PushFront(i)
			l.items["good"] = i
			l.list.PushFront(&lruItem{key: []byte("bad")})

			size := l.Get([]byte("good"))
			Ω(size).Should(Equal(int64(100)))
			Ω(l.list.Front()).Should(Equal(i.elem))
		})
	})

	Context("PutAndEvict", func() {

		It("should insert a new item successfully", func() {
			l := DefaultBasicLRU(0)
			evicted, bytes := l.PutAndEvict([]byte("key"), 100)
			Ω(evicted).Should(BeNil())
			Ω(bytes).Should(Equal(int64(0)))
			Ω(l.size).Should(Equal(int64(100)))
			Ω(l.Len()).Should(Equal(int64(1)))
			i := l.items["key"]
			Ω(string(i.key)).Should(Equal("key"))
			Ω(i.size).Should(Equal(int64(100)))
			Ω(i.elem).Should(Equal(l.list.Front()))
		})

		It("should update an item with a new size successfully", func() {
			l := DefaultBasicLRU(0)
			l.PutAndEvict([]byte("key"), 100)
			l.PutAndEvict([]byte("key2"), 150)
			evicted, bytes := l.PutAndEvict([]byte("key"), 200)
			Ω(evicted).Should(BeNil())
			Ω(bytes).Should(Equal(int64(0)))
			Ω(l.size).Should(Equal(int64(150 + 200)))
			Ω(l.Len()).Should(Equal(int64(2)))
			i := l.items["key"]
			Ω(string(i.key)).Should(Equal("key"))
			Ω(i.size).Should(Equal(int64(200)))
			Ω(i.elem).Should(Equal(l.list.Front()))
		})

		It("should prune items off of the LRU to the prune capacity", func() {
			l := NewBasicLRU(1000, 0.5)
			for i := 0; i < 6; i++ {
				evicted, bytes := l.PutAndEvict([]byte(strconv.Itoa(i)), 150)
				Ω(evicted).Should(BeNil())
				Ω(bytes).Should(Equal(int64(0)))
			}
			evicted, bytes := l.PutAndEvict([]byte("6"), 150)
			Ω(evicted).Should(HaveLen(4))
			Ω(bytes).Should(Equal(int64(150 * 4)))
			Ω(l.size).Should(Equal(int64(450)))
			Ω(l.Len()).Should(Equal(int64(3)))
			last := l.items["4"]
			Ω(last).ShouldNot(BeNil())
			Ω(last.elem).Should(Equal(l.list.Back()))
		})
	})

	Context("Empty", func() {

		It("should completely empty the LRU", func() {
			l := DefaultBasicLRU(0)
			l.PutAndEvict([]byte("key"), 100)
			Ω(l.Size()).Should(Equal(int64(100)))
			Ω(l.Len()).Should(Equal(int64(1)))

			l.Empty()
			Ω(l.Size()).Should(Equal(int64(0)))
			Ω(l.Len()).Should(Equal(int64(0)))
			Ω(l.items).Should(HaveLen(0))
		})
	})

	Context("PutOnStartup", func() {

		It("should insert items into the LRU and discard items when past its capacity", func() {
			l := DefaultBasicLRU(0)
			for i := 0; i < 10; i++ {
				ok := l.PutOnStartup([]byte(strconv.Itoa(i)), 100)
				Ω(ok).Should(BeTrue())
			}
			ok := l.PutOnStartup([]byte("10"), 100)
			Ω(ok).Should(BeFalse())
			Ω(l.size).Should(Equal(l.cap))
			Ω(l.Len()).Should(Equal(int64(10)))
		})
	})

	Context("evict", func() {

		It("should return nil when the list is empty", func() {
			l := DefaultBasicLRU(0)
			l.size = 1200
			evicted, bytes := l.evict()
			Ω(evicted).Should(HaveLen(0))
			Ω(bytes).Should(Equal(int64(0)))
			Ω(l.Len()).Should(Equal(int64(0)))
		})
	})
})

// Benchmark getting an existing key with a BasicLRU.
func BenchmarkBasicLRUGet(b *testing.B) {
	l := DefaultBasicLRU(1e6)
	key := []byte("key")
	l.PutOnStartup(key, 200)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.Get(key)
	}
}

// Benchmark inserting/evicting items with a BasicLRU.
func BenchmarkBasicLRUPutAndEvict(b *testing.B) {
	l := DefaultBasicLRU(1e6)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.PutAndEvict([]byte(strconv.Itoa(i)), 100)
	}
}
