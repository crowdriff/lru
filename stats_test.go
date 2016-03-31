package lru

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Stats", func() {

	Context("Stats", func() {

		It("should retrieve the current stats", func() {
			l := newDefaultLRU()
			defer closeBoltDB(l)
			setTestStats(l)
			s := l.Stats()
			verifyTestStats(s)
		})

		It("should reset the stats and return the stats before the reset", func() {
			l := newDefaultLRU()
			defer closeBoltDB(l)
			setTestStats(l)
			s := l.ResetStats()
			verifyTestStats(s)
			s = l.Stats()
			Ω(s.StartTime.IsZero()).Should(BeFalse())
			Ω(s.Uptime).Should(BeNumerically(">", 0))
			Ω(s.Hits).Should(Equal(int64(0)))
			Ω(s.Misses).Should(Equal(int64(0)))
			Ω(s.GetBytes).Should(Equal(int64(0)))
			Ω(s.Puts).Should(Equal(int64(0)))
			Ω(s.PutBytes).Should(Equal(int64(0)))
			Ω(s.Evicted).Should(Equal(int64(0)))
			Ω(s.EvictedBytes).Should(Equal(int64(0)))
			Ω(s.Size).Should(Equal(int64(600)))
			Ω(s.Capacity).Should(Equal(int64(1000)))
			Ω(s.NumItems).Should(Equal(int64(2)))
		})
	})
})

func setTestStats(l *LRU) {
	l.lru.lruHot.pushToFront(&listItem{size: 400, key: []byte("1")})
	l.lru.lruWarm.pushToFront(&listItem{size: 200, key: []byte("2")})
	l.lru.items["key"] = &listItem{}
	l.hits = 1
	l.misses = 2
	l.bget = 3
	l.puts = 4
	l.bput = 5
	l.evicted = 6
	l.bevicted = 7
}

func verifyTestStats(s Stats) {
	Ω(s.StartTime.IsZero()).Should(BeFalse())
	Ω(s.Uptime).Should(BeNumerically(">", 0))
	Ω(s.Hits).Should(Equal(int64(1)))
	Ω(s.Misses).Should(Equal(int64(2)))
	Ω(s.GetBytes).Should(Equal(int64(3)))
	Ω(s.Puts).Should(Equal(int64(4)))
	Ω(s.PutBytes).Should(Equal(int64(5)))
	Ω(s.Evicted).Should(Equal(int64(6)))
	Ω(s.EvictedBytes).Should(Equal(int64(7)))
	Ω(s.Size).Should(Equal(int64(600)))
	Ω(s.Capacity).Should(Equal(int64(1000)))
	Ω(s.NumItems).Should(Equal(int64(2)))
}
