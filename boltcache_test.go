package lru

import (
	"strconv"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Boltcache", func() {

	Context("openBoltDB", func() {

		It("should return an error when attempting to open an invalid path", func() {
			l := NewLRU(0, "///", "", nil)
			defer closeBoltDB(l)
			err := l.openBoltDB()
			Ω(err).Should(HaveOccurred())
		})
	})

	Context("fillCacheFromBolt", func() {

		It("should attempt to fill the cache, but no data currently exists", func() {
			l := NewLRU(0, "", "", nil)
			defer l.Close()
			err := l.openBoltDB()
			Ω(err).ShouldNot(HaveOccurred())
			Ω(l.items).Should(HaveLen(0))
		})

		It("should fill the cache with all data in the bolt database and delete items exceeding the capacity", func() {
			// insert 1200 bytes into the bolt database
			l := NewLRU(1000, "", "", nil)
			err := l.Open()
			Ω(err).ShouldNot(HaveOccurred())
			for i := 0; i < 3; i++ {
				err = l.putIntoBolt([]byte(strconv.Itoa(i)), make([]byte, 400))
				Ω(err).ShouldNot(HaveOccurred())
			}
			closeBoltDB(l)

			// attempt to open and fill LRU
			l = newDefaultLRU()
			defer closeBoltDB(l)
			Ω(l.items).Should(HaveLen(2))
			_, err = l.Get([]byte("3"))
			Ω(err).Should(MatchError(errNoStore))
		})
	})

	Context("getFromBolt", func() {

		It("should return nil when trying to retrieve a key that doesn't exist", func() {
			l := newDefaultLRU()
			defer closeBoltDB(l)
			v := l.getFromBolt([]byte("key"))
			Ω(v).Should(BeNil())
		})

		It("should return the value from bolt", func() {
			l := newDefaultLRU()
			defer closeBoltDB(l)
			err := l.putIntoBolt([]byte("key"), []byte("value"))
			Ω(err).ShouldNot(HaveOccurred())
			v := l.getFromBolt([]byte("key"))
			Ω(string(v)).Should(Equal("value"))
		})

		It("should return nil when the db.View function returns an error", func() {
			l := newDefaultLRU()
			defer closeBoltDB(l)
			err := l.putIntoBolt([]byte("key"), []byte("value"))
			Ω(err).ShouldNot(HaveOccurred())
			l.db.Close()
			v := l.getFromBolt([]byte("key"))
			Ω(v).Should(BeNil())
		})
	})

	Context("getBufFromBolt", func() {

		It("should return nil when trying to retrieve a key that doesn't exist", func() {
			l := newDefaultLRU()
			defer closeBoltDB(l)
			b := l.getBufFromBolt([]byte("key"))
			Ω(b).Should(BeNil())
		})

		It("should return a buffer for the value from bolt", func() {
			l := newDefaultLRU()
			defer closeBoltDB(l)
			err := l.putIntoBolt([]byte("key"), []byte("value"))
			Ω(err).ShouldNot(HaveOccurred())
			b := l.getBufFromBolt([]byte("key"))
			Ω(b.String()).Should(Equal("value"))
		})

		It("should return nil when the db.View function returns an error", func() {
			l := newDefaultLRU()
			defer closeBoltDB(l)
			err := l.putIntoBolt([]byte("key"), []byte("value"))
			Ω(err).ShouldNot(HaveOccurred())
			l.db.Close()
			b := l.getBufFromBolt([]byte("key"))
			Ω(b).Should(BeNil())
		})
	})

	Context("emptyBolt", func() {

		It("should empty the bolt database's bucket", func() {
			// create LRU and insert key
			l := newDefaultLRU()
			defer closeBoltDB(l)
			err := l.putIntoBolt([]byte("key"), []byte("value"))
			Ω(err).ShouldNot(HaveOccurred())
			v := l.getFromBolt([]byte("key"))
			Ω(string(v)).Should(Equal("value"))

			// empty the database
			err = l.emptyBolt()
			Ω(err).ShouldNot(HaveOccurred())
			v = l.getFromBolt([]byte("key"))
			Ω(v).Should(BeNil())
		})
	})

	Context("deleteFromBolt", func() {

		It("should delete the provided keys from the bolt database", func() {
			// create LRU and insert keys
			l := newDefaultLRU()
			defer closeBoltDB(l)
			var toRemove [][]byte
			for i := 0; i < 4; i++ {
				key := []byte(strconv.Itoa(i))
				toRemove = append(toRemove, key)
				err := l.putIntoBolt(key, []byte("value"))
				Ω(err).ShouldNot(HaveOccurred())
			}

			// delete 3 from bolt
			err := l.deleteFromBolt(toRemove[:3])
			Ω(err).ShouldNot(HaveOccurred())
			for i := 0; i < 3; i++ {
				v := l.getFromBolt(toRemove[i])
				Ω(v).Should(BeNil())
			}
			v := l.getFromBolt(toRemove[3])
			Ω(v).ShouldNot(BeNil())
			Ω(string(v)).Should(Equal("value"))
		})
	})
})
