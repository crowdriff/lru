package lru

import "container/list"

// BasicLRU is an implementation of a basic least recently used (LRU) cache.
// For more information, see: https://en.wikipedia.org/wiki/Page_replacement_algorithm#Least_recently_used
type BasicLRU struct {
	items    map[string]*lruItem
	list     *list.List
	cap      int64
	size     int64
	pruneCap int64
}

// DefaultBasicLRU returns a new BasicLRU instance with the provided capacity
// and an eviction ratio of 0.1%.
func DefaultBasicLRU(cap int64) *BasicLRU {
	return NewBasicLRU(cap, 0.001)
}

// NewBasicLRU returns a new BasicLRU with the provided capacity, and eviction
// ratio.
//
// evictRatio represents the percentage of items (based on size) that should be
// evicted when the LRU's capacity is exceeded.
func NewBasicLRU(cap int64, evictRatio float64) *BasicLRU {
	// capacity should be at least 1000 bytes
	if cap < 1000 {
		cap = 1000
	}
	// evict ratio must be between 0.0 & 1.0
	if evictRatio < 0.0 {
		evictRatio = 0.0
	} else if evictRatio > 1.0 {
		evictRatio = 1.0
	}
	return &BasicLRU{
		items:    make(map[string]*lruItem, 1e4),
		list:     list.New(),
		cap:      cap,
		pruneCap: int64((1.0 - evictRatio) * float64(cap)),
	}
}

// lruItem represents a single item in the eviction list.
type lruItem struct {
	key  []byte        // item's key
	size int64         // size of the item's value in bytes
	elem *list.Element // linked list pointer
}

// Get returns the size of the value corresponding to the provided key, or -1
// if the key doesn't exist in the LRU.
func (bl *BasicLRU) Get(key []byte) int64 {
	if i, ok := bl.items[string(key)]; ok {
		bl.list.MoveToFront(i.elem)
		return i.size
	}
	return -1
}

// PutAndEvict inserts the provided key and value size into the LRU and returns
// a slice of keys that have been evicted and total bytes evicted.
func (bl *BasicLRU) PutAndEvict(key []byte, size int64) ([][]byte, int64) {
	if i, ok := bl.items[string(key)]; ok {
		bl.size += (size - i.size)
		i.size = size
		bl.list.MoveToFront(i.elem)
		return bl.prune()
	}
	i := &lruItem{key: key, size: size}
	bl.size += i.size
	i.elem = bl.list.PushFront(i)
	bl.items[string(key)] = i
	return bl.prune()
}

// Cap returns the total capacity of the LRU in bytes.
func (bl *BasicLRU) Cap() int64 {
	return bl.cap
}

// Len returns the number of items in the LRU.
func (bl *BasicLRU) Len() int64 {
	return int64(bl.list.Len())
}

// Size returns the total number of bytes in the LRU.
func (bl *BasicLRU) Size() int64 {
	return bl.size
}

// Empty completely empties the LRU.
func (bl *BasicLRU) Empty() {
	bl.items = make(map[string]*lruItem)
	bl.list = list.New()
	bl.size = 0
}

// PutOnStartup adds the provided key and value size into the LRU as an initial
// item. All items are inserted into the LRU until full, where items are
// dropped and 'false' is returned.
func (bl *BasicLRU) PutOnStartup(key []byte, size int64) bool {
	i := &lruItem{key: key, size: size}
	if bl.size+size <= bl.cap {
		bl.size += size
		i.elem = bl.list.PushFront(i)
		bl.items[string(key)] = i
		return true
	}
	return false
}

// prune evicts items off of the back of the LRU if the LRU's size exceeds its
// capacity. It returns a slice of keys that have been evicted and the total
// number of bytes evicted.
func (bl *BasicLRU) prune() ([][]byte, int64) {
	if bl.size <= bl.cap {
		return nil, 0
	}
	return bl.evict()
}

// evict evicts items off of the back of the LRU until the LRU's size is less
// than or equal to the 'prune capacity'. It returns a slice of keys that have
// been evicted and the total number of bytes evicted.
func (bl *BasicLRU) evict() ([][]byte, int64) {
	var bevicted int64
	var evicted [][]byte
	for bl.size > bl.pruneCap {
		tail := bl.list.Back()
		if tail == nil {
			return evicted, bevicted
		}
		i := bl.list.Remove(tail).(*lruItem)
		delete(bl.items, string(i.key))
		bl.size -= i.size
		bevicted += i.size
		evicted = append(evicted, i.key)
	}
	return evicted, bevicted
}
