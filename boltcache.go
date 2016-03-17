package lru

import (
	"github.com/boltdb/bolt"
)

// openBoltDB opens the boltDB database and assigns it to the LRU as "db".
func (l *LRU) openBoltDB() error {
	db, err := bolt.Open(l.dbPath, 0666, nil)
	if err != nil {
		return err
	}
	l.db = db
	return l.fillCacheFromBolt()
}

// fillCacheFromBolt fills the cache with all of the values currently in the
// bolt database. If the cache reaches its capacity, subsequent values are
// deleted.
func (l *LRU) fillCacheFromBolt() error {
	// fill the LRU with existing data
	return l.db.Update(func(tx *bolt.Tx) error {
		// create the bucket if it doesn't exist
		b, err := tx.CreateBucketIfNotExists(l.bName)
		if err != nil {
			return err
		}
		// cycle through all entries and add them to the LRU
		c := b.Cursor()
		l.mu.Lock()
		defer l.mu.Unlock()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			size := int64(len(v))
			if l.remain-size < 0 {
				if err := c.Delete(); err != nil {
					return err
				}
				continue
			}
			key := make([]byte, len(k))
			copy(key, k)
			l.addItemWithMu(key, size)
		}
		return nil
	})
}

// getFromBolt returns the value corresponding to the provided key from the
// bolt database, or nil if the key doesn't exist.
func (l *LRU) getFromBolt(key []byte) []byte {
	var buf []byte
	err := l.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(l.bName)
		v := b.Get(key)
		if v == nil {
			return nil
		}
		buf = make([]byte, len(v))
		copy(buf, v)
		return nil
	})
	if err != nil {
		return nil
	}
	return buf
}

// putIntoBolt writes the provided key and value into the bolt database and
// returns any error encountered.
func (l *LRU) putIntoBolt(key, val []byte) error {
	return l.db.Batch(func(tx *bolt.Tx) error {
		b := tx.Bucket(l.bName)
		return b.Put(key, val)
	})
}

// emptyBolt completely empties the bolt database and returns any error
// encountered.
func (l *LRU) emptyBolt() error {
	return l.db.Update(func(tx *bolt.Tx) error {
		if err := tx.DeleteBucket(l.bName); err != nil {
			return err
		}
		_, err := tx.CreateBucket(l.bName)
		return err
	})
}

// deleteFromBolt deletes the provided slice of keys from the bolt database and
// returns any error encountered.
func (l *LRU) deleteFromBolt(keys [][]byte) error {
	return l.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(l.bName)
		for _, key := range keys {
			// ignore a delete error to avoid having the entire transaction fail
			_ = b.Delete(key)
		}
		return nil
	})
}
