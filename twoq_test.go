package lru

import (
	"strconv"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Twoq", func() {

	Context("newTwoQ", func() {

		It("should create a new twoQ LRU with the provided options", func() {
			tq := newTwoQ(0, -1.0, -1.0, -1.0)
			Ω(tq.items).ShouldNot(BeNil())
			Ω(tq.items).Should(HaveLen(0))
			Ω(tq.cap).Should(Equal(int64(1000)))
			Ω(tq.pruneCap).Should(Equal(tq.cap))
			Ω(tq.lruHot.cap).Should(Equal(int64(1000)))
			Ω(tq.lruWarm.cap).Should(Equal(int64(0)))
			Ω(tq.lruCold.cap).Should(Equal(int64(0)))
		})

		It("should create a new twoQ LRU with the provided options", func() {
			tq := newTwoQ(10e6, 1.5, 1.5, 0.5)
			Ω(tq.items).ShouldNot(BeNil())
			Ω(tq.items).Should(HaveLen(0))
			Ω(tq.size()).Should(Equal(int64(0)))
			Ω(tq.cap).Should(Equal(int64(10e6)))
			Ω(tq.pruneCap).Should(Equal(int64(0)))
			Ω(tq.lruHot.cap).Should(Equal(int64(0)))
			Ω(tq.lruWarm.cap).Should(Equal(int64(10e6)))
			Ω(tq.lruCold.cap).Should(Equal(int64(10e6 / 2)))
		})
	})

	Context("get", func() {

		It("should return -1 when the key doesn't exist in the LRU", func() {
			tq := newTwoQ(0, 0.0, 0.25, 0.5)
			size := tq.get([]byte("key"))
			Ω(size).Should(Equal(int64(-1)))
		})

		It("should return an item from the warm LRU", func() {
			tq := newTwoQ(0, 0.0, 0.25, 0.5)
			evicted := tq.putAndEvict([]byte("key"), 100)
			Ω(evicted).Should(HaveLen(0))
			Ω(tq.lruWarm.list.Len()).Should(Equal(1))
			size := tq.get([]byte("key"))
			Ω(size).Should(Equal(int64(100)))
			Ω(tq.lruWarm.list.Len()).Should(Equal(0))
			Ω(tq.lruHot.list.Len()).Should(Equal(1))
			size = tq.get([]byte("key"))
			Ω(size).Should(Equal(int64(100)))
		})

		It("should return an item from the hot LRU", func() {
			tq := newTwoQ(0, 0.0, 0.25, 0.5)
			for i := 0; i < 3; i++ {
				evicted := tq.putAndEvict([]byte(strconv.Itoa(i)), 100)
				Ω(evicted).Should(HaveLen(0))
				size := tq.get([]byte(strconv.Itoa(i)))
				Ω(size).Should(Equal(int64(100)))
			}
			Ω(tq.lruHot.list.Len()).Should(Equal(3))
			Ω(tq.lruWarm.list.Len()).Should(Equal(0))
			Ω(isFront(hot, tq, "2")).Should(BeTrue())

			size := tq.get([]byte("0"))
			Ω(size).Should(Equal(int64(100)))
			Ω(isFront(hot, tq, "0")).Should(BeTrue())
		})
	})

	Context("putAndEvict", func() {

		It("should insert a new item", func() {
			tq := newTwoQ(0, 0.0, 0.25, 0.5)
			evicted := tq.putAndEvict([]byte("key"), 100)
			Ω(evicted).Should(HaveLen(0))
			Ω(tq.lruWarm.list.Len()).Should(Equal(1))
			Ω(tq.lruHot.list.Len()).Should(Equal(0))
		})

		It("should insert an item into hot from the cold LRU", func() {
			tq := newTwoQ(0, 0.0, 0.25, 0.5)
			evicted := tq.putAndEvict([]byte("key"), 100)
			Ω(evicted).Should(HaveLen(0))
			i := tq.items["key"]
			tq.lruWarm.list.Remove(i.elem)
			tq.lruCold.pushToFront(i)

			evicted = tq.putAndEvict([]byte("key"), 200)
			Ω(evicted).Should(HaveLen(0))
			Ω(tq.lruCold.list.Len()).Should(Equal(0))
			Ω(tq.lruWarm.list.Len()).Should(Equal(0))
			Ω(tq.lruHot.list.Len()).Should(Equal(1))
			Ω(tq.items["key"].size).Should(Equal(int64(200)))
		})

		It("should insert an item into hot from the warm LRU", func() {
			tq := newTwoQ(0, 0.0, 0.25, 0.5)
			evicted := tq.putAndEvict([]byte("key"), 100)
			Ω(evicted).Should(HaveLen(0))

			evicted = tq.putAndEvict([]byte("key"), 200)
			Ω(evicted).Should(HaveLen(0))
			Ω(tq.lruCold.list.Len()).Should(Equal(0))
			Ω(tq.lruWarm.list.Len()).Should(Equal(0))
			Ω(tq.lruHot.list.Len()).Should(Equal(1))
			Ω(tq.items["key"].size).Should(Equal(int64(200)))
		})

		It("should move a hot item to the front of the list", func() {
			tq := newTwoQ(0, 0.0, 0.25, 0.5)
			for i := 0; i < 2; i++ {
				evicted := tq.putAndEvict([]byte(strconv.Itoa(i)), 100)
				Ω(evicted).Should(HaveLen(0))
				size := tq.get([]byte(strconv.Itoa(i)))
				Ω(size).Should(Equal(int64(100)))
			}
			Ω(tq.lruHot.list.Len()).Should(Equal(2))
			Ω(isFront(hot, tq, "1")).Should(BeTrue())

			evicted := tq.putAndEvict([]byte("0"), 200)
			Ω(evicted).Should(HaveLen(0))
			Ω(tq.lruCold.list.Len()).Should(Equal(0))
			Ω(tq.lruWarm.list.Len()).Should(Equal(0))
			Ω(tq.lruHot.list.Len()).Should(Equal(2))
			Ω(isFront(hot, tq, "0")).Should(BeTrue())
			Ω(tq.items["0"].size).Should(Equal(int64(200)))
		})
	})

	Context("prune", func() {

		It("should prune from the warm lru", func() {
			tq := newTwoQ(0, 0.0, 0.25, 0.5)
			for i := 0; i < 3; i++ {
				evicted := tq.putAndEvict([]byte(strconv.Itoa(i)), 300)
				Ω(evicted).Should(HaveLen(0))
			}
			evicted := tq.putAndEvict([]byte("3"), 300)
			Ω(evicted).Should(HaveLen(1))
			Ω(string(evicted[0])).Should(Equal("0"))
			Ω(tq.lruCold.list.Len()).Should(Equal(1))
			Ω(tq.lruWarm.list.Len()).Should(Equal(3))
		})

		It("should prune from the hot lru", func() {
			tq := newTwoQ(0, 0.0, 0.25, 0.5)
			for i := 0; i < 3; i++ {
				evicted := tq.putAndEvict([]byte(strconv.Itoa(i)), 300)
				Ω(evicted).Should(HaveLen(0))
				size := tq.get([]byte(strconv.Itoa(i)))
				Ω(size).Should(Equal(int64(300)))
			}
			Ω(tq.lruHot.list.Len()).Should(Equal(3))

			evicted := tq.putAndEvict([]byte("3"), 150)
			Ω(evicted).Should(HaveLen(1))
			Ω(string(evicted[0])).Should(Equal("0"))
			Ω(tq.lruCold.list.Len()).Should(Equal(1))
			Ω(tq.lruWarm.list.Len()).Should(Equal(1))
			Ω(tq.lruHot.list.Len()).Should(Equal(2))
		})
	})

	Context("pruneCold", func() {

		It("should prune nothing from the cold lru", func() {
			tq := newTwoQ(0, 0.0, 0.25, 0.0)
			tq.lruCold.size = 100
			Ω(tq.lruCold.list.Len()).Should(Equal(0))
			tq.pruneCold()
			Ω(tq.lruCold.list.Len()).Should(Equal(0))
		})

		It("should prune from the cold lru", func() {
			tq := newTwoQ(0, 0.0, 0.25, 0.5)
			for i := 0; i < 4; i++ {
				itm := &listItem{
					key:  []byte(strconv.Itoa(i)),
					size: 150,
				}
				tq.items[strconv.Itoa(i)] = itm
				tq.lruCold.pushToFront(itm)
			}
			Ω(tq.lruCold.list.Len()).Should(Equal(4))

			tq.pruneCold()
			Ω(tq.lruCold.list.Len()).Should(Equal(3))
		})
	})

	Context("evict", func() {

		It("should return nil when the list is empty", func() {
			tq := newTwoQ(0, 0.0, 0.25, 0.5)
			tq.lruHot.size = 1200
			evicted := tq.lruHot.evict()
			Ω(evicted).Should(HaveLen(0))
			Ω(tq.lruHot.list.Len()).Should(Equal(0))
		})
	})
})

func isFront(status uint8, tq *twoQ, key string) bool {
	if i, ok := tq.items[key]; ok {
		if i.status != status {
			return false
		}
		return i.elem.Prev() == nil
	}
	return false
}
