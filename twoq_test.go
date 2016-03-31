package lru

import (
	"strconv"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Twoq", func() {

	Context("NewTwoQ", func() {

		It("should create a TwoQ LRU with the default options", func() {
			tq := DefaultTwoQ(0)
			Ω(tq.items).ShouldNot(BeNil())
			Ω(tq.items).Should(HaveLen(0))
			Ω(tq.cap).Should(Equal(int64(1000)))
			Ω(tq.pruneCap).Should(Equal(int64(999)))
			Ω(tq.lruHot.cap).Should(Equal(int64(750)))
			Ω(tq.lruWarm.cap).Should(Equal(int64(250)))
			Ω(tq.lruCold.cap).Should(Equal(int64(500)))
		})

		It("should create a new TwoQ LRU with the provided options", func() {
			tq := NewTwoQ(0, -1.0, -1.0, -1.0)
			Ω(tq.items).ShouldNot(BeNil())
			Ω(tq.items).Should(HaveLen(0))
			Ω(tq.cap).Should(Equal(int64(1000)))
			Ω(tq.pruneCap).Should(Equal(tq.cap))
			Ω(tq.lruHot.cap).Should(Equal(int64(1000)))
			Ω(tq.lruWarm.cap).Should(Equal(int64(0)))
			Ω(tq.lruCold.cap).Should(Equal(int64(0)))
		})

		It("should create a new TwoQ LRU with the provided options", func() {
			tq := NewTwoQ(10e6, 1.5, 1.5, 0.5)
			Ω(tq.items).ShouldNot(BeNil())
			Ω(tq.items).Should(HaveLen(0))
			Ω(tq.Size()).Should(Equal(int64(0)))
			Ω(tq.cap).Should(Equal(int64(10e6)))
			Ω(tq.pruneCap).Should(Equal(int64(0)))
			Ω(tq.lruHot.cap).Should(Equal(int64(0)))
			Ω(tq.lruWarm.cap).Should(Equal(int64(10e6)))
			Ω(tq.lruCold.cap).Should(Equal(int64(10e6 / 2)))
		})
	})

	Context("Get", func() {

		It("should return -1 when the key doesn't exist in the LRU", func() {
			tq := NewTwoQ(0, 0.0, 0.25, 0.5)
			size := tq.Get([]byte("key"))
			Ω(size).Should(Equal(int64(-1)))
		})

		It("should return an item from the warm LRU", func() {
			tq := NewTwoQ(0, 0.0, 0.25, 0.5)
			evicted, bytes := tq.PutAndEvict([]byte("key"), 100)
			Ω(evicted).Should(HaveLen(0))
			Ω(bytes).Should(Equal(int64(0)))
			Ω(tq.lruWarm.list.Len()).Should(Equal(1))
			size := tq.Get([]byte("key"))
			Ω(size).Should(Equal(int64(100)))
			Ω(tq.lruWarm.list.Len()).Should(Equal(0))
			Ω(tq.lruHot.list.Len()).Should(Equal(1))
			size = tq.Get([]byte("key"))
			Ω(size).Should(Equal(int64(100)))
		})

		It("should return an item from the hot LRU", func() {
			tq := NewTwoQ(0, 0.0, 0.25, 0.5)
			for i := 0; i < 3; i++ {
				evicted, bytes := tq.PutAndEvict([]byte(strconv.Itoa(i)), 100)
				Ω(evicted).Should(HaveLen(0))
				Ω(bytes).Should(Equal(int64(0)))
				size := tq.Get([]byte(strconv.Itoa(i)))
				Ω(size).Should(Equal(int64(100)))
			}
			Ω(tq.lruHot.list.Len()).Should(Equal(3))
			Ω(tq.lruWarm.list.Len()).Should(Equal(0))
			Ω(isFront(twoQHot, tq, "2")).Should(BeTrue())

			size := tq.Get([]byte("0"))
			Ω(size).Should(Equal(int64(100)))
			Ω(isFront(twoQHot, tq, "0")).Should(BeTrue())
		})
	})

	Context("PutAndEvict", func() {

		It("should insert a new item", func() {
			tq := NewTwoQ(0, 0.0, 0.25, 0.5)
			evicted, bytes := tq.PutAndEvict([]byte("key"), 100)
			Ω(evicted).Should(HaveLen(0))
			Ω(bytes).Should(Equal(int64(0)))
			Ω(tq.lruWarm.list.Len()).Should(Equal(1))
			Ω(tq.lruHot.list.Len()).Should(Equal(0))
		})

		It("should insert an item into hot from the cold LRU", func() {
			tq := NewTwoQ(0, 0.0, 0.25, 0.5)
			evicted, bytes := tq.PutAndEvict([]byte("key"), 100)
			Ω(evicted).Should(HaveLen(0))
			Ω(bytes).Should(Equal(int64(0)))
			i := tq.items["key"]
			tq.lruWarm.list.Remove(i.elem)
			tq.lruCold.pushToFront(i)

			evicted, bytes = tq.PutAndEvict([]byte("key"), 200)
			Ω(evicted).Should(HaveLen(0))
			Ω(bytes).Should(Equal(int64(0)))
			Ω(tq.lruCold.list.Len()).Should(Equal(0))
			Ω(tq.lruWarm.list.Len()).Should(Equal(0))
			Ω(tq.lruHot.list.Len()).Should(Equal(1))
			Ω(tq.items["key"].size).Should(Equal(int64(200)))
		})

		It("should insert an item into hot from the warm LRU", func() {
			tq := NewTwoQ(0, 0.0, 0.25, 0.5)
			evicted, bytes := tq.PutAndEvict([]byte("key"), 100)
			Ω(evicted).Should(HaveLen(0))
			Ω(bytes).Should(Equal(int64(0)))

			evicted, bytes = tq.PutAndEvict([]byte("key"), 200)
			Ω(evicted).Should(HaveLen(0))
			Ω(bytes).Should(Equal(int64(0)))
			Ω(tq.lruCold.list.Len()).Should(Equal(0))
			Ω(tq.lruWarm.list.Len()).Should(Equal(0))
			Ω(tq.lruHot.list.Len()).Should(Equal(1))
			Ω(tq.items["key"].size).Should(Equal(int64(200)))
		})

		It("should move a hot item to the front of the list", func() {
			tq := NewTwoQ(0, 0.0, 0.25, 0.5)
			for i := 0; i < 2; i++ {
				evicted, bytes := tq.PutAndEvict([]byte(strconv.Itoa(i)), 100)
				Ω(evicted).Should(HaveLen(0))
				Ω(bytes).Should(Equal(int64(0)))
				size := tq.Get([]byte(strconv.Itoa(i)))
				Ω(size).Should(Equal(int64(100)))
			}
			Ω(tq.lruHot.list.Len()).Should(Equal(2))
			Ω(isFront(twoQHot, tq, "1")).Should(BeTrue())

			evicted, bytes := tq.PutAndEvict([]byte("0"), 200)
			Ω(evicted).Should(HaveLen(0))
			Ω(bytes).Should(Equal(int64(0)))
			Ω(tq.lruCold.list.Len()).Should(Equal(0))
			Ω(tq.lruWarm.list.Len()).Should(Equal(0))
			Ω(tq.lruHot.list.Len()).Should(Equal(2))
			Ω(isFront(twoQHot, tq, "0")).Should(BeTrue())
			Ω(tq.items["0"].size).Should(Equal(int64(200)))
		})
	})

	Context("Empty", func() {

		It("should completely empty the LRU", func() {
			tq := DefaultTwoQ(0)
			iCold := &listItem{
				key:    []byte("1"),
				status: twoQCold,
				size:   10,
			}
			tq.lruCold.pushToFront(iCold)
			tq.items["1"] = iCold
			iWarm := &listItem{
				key:    []byte("2"),
				status: twoQWarm,
				size:   10,
			}
			tq.lruWarm.pushToFront(iWarm)
			tq.items["2"] = iWarm
			iHot := &listItem{
				key:    []byte("3"),
				status: twoQHot,
				size:   10,
			}
			tq.lruHot.pushToFront(iHot)
			tq.items["3"] = iHot

			tq.Empty()
			Ω(tq.items).Should(HaveLen(0))
			Ω(tq.lruCold.size).Should(Equal(int64(0)))
			Ω(tq.lruCold.list.Len()).Should(Equal(0))
			Ω(tq.lruWarm.size).Should(Equal(int64(0)))
			Ω(tq.lruWarm.list.Len()).Should(Equal(0))
			Ω(tq.lruHot.size).Should(Equal(int64(0)))
			Ω(tq.lruHot.list.Len()).Should(Equal(0))
		})
	})

	Context("PutOnStartup", func() {

		It("should push items successfully into the LRU", func() {
			tq := DefaultTwoQ(0)
			for i := 0; i < 3; i++ {
				ok := tq.PutOnStartup([]byte(strconv.Itoa(i)), 300)
				Ω(ok).Should(BeTrue())
			}
			ok := tq.PutOnStartup([]byte("3"), 200)
			Ω(ok).Should(BeFalse())
			Ω(tq.lruWarm.list.Len()).Should(Equal(3))
			Ω(tq.lruWarm.size).Should(Equal(int64(900)))
			Ω(tq.lruCold.list.Len()).Should(Equal(1))
			Ω(tq.lruCold.size).Should(Equal(int64(200)))
		})
	})

	Context("prune", func() {

		It("should prune from the warm lru", func() {
			tq := NewTwoQ(0, 0.0, 0.25, 0.5)
			for i := 0; i < 3; i++ {
				evicted, bytes := tq.PutAndEvict([]byte(strconv.Itoa(i)), 300)
				Ω(evicted).Should(HaveLen(0))
				Ω(bytes).Should(Equal(int64(0)))
			}
			evicted, bytes := tq.PutAndEvict([]byte("3"), 300)
			Ω(evicted).Should(HaveLen(1))
			Ω(bytes).Should(Equal(int64(300)))
			Ω(string(evicted[0])).Should(Equal("0"))
			Ω(tq.lruCold.list.Len()).Should(Equal(1))
			Ω(tq.lruWarm.list.Len()).Should(Equal(3))
		})

		It("should prune from the hot lru", func() {
			tq := NewTwoQ(0, 0.0, 0.25, 0.5)
			for i := 0; i < 3; i++ {
				evicted, bytes := tq.PutAndEvict([]byte(strconv.Itoa(i)), 300)
				Ω(evicted).Should(HaveLen(0))
				Ω(bytes).Should(Equal(int64(0)))
				size := tq.Get([]byte(strconv.Itoa(i)))
				Ω(size).Should(Equal(int64(300)))
			}
			Ω(tq.lruHot.list.Len()).Should(Equal(3))

			evicted, bytes := tq.PutAndEvict([]byte("3"), 150)
			Ω(evicted).Should(HaveLen(1))
			Ω(bytes).Should(Equal(int64(300)))
			Ω(string(evicted[0])).Should(Equal("0"))
			Ω(tq.lruCold.list.Len()).Should(Equal(1))
			Ω(tq.lruWarm.list.Len()).Should(Equal(1))
			Ω(tq.lruHot.list.Len()).Should(Equal(2))
		})
	})

	Context("pruneCold", func() {

		It("should prune nothing from the cold lru", func() {
			tq := NewTwoQ(0, 0.0, 0.25, 0.0)
			tq.lruCold.size = 100
			Ω(tq.lruCold.list.Len()).Should(Equal(0))
			tq.pruneCold()
			Ω(tq.lruCold.list.Len()).Should(Equal(0))
		})

		It("should prune from the cold lru", func() {
			tq := NewTwoQ(0, 0.0, 0.25, 0.5)
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
			tq := NewTwoQ(0, 0.0, 0.25, 0.5)
			tq.lruHot.size = 1200
			evicted, bytes := tq.lruHot.evict()
			Ω(evicted).Should(HaveLen(0))
			Ω(bytes).Should(Equal(int64(0)))
			Ω(tq.lruHot.list.Len()).Should(Equal(0))
		})
	})
})

func isFront(status uint8, tq *TwoQ, key string) bool {
	if i, ok := tq.items[key]; ok {
		if i.status != status {
			return false
		}
		return i.elem.Prev() == nil
	}
	return false
}
