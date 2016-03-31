package lru

import "container/list"

// TwoQ is an implementation of the 2Q LRU algorithm, as defined by Theodore
// Johnson and Dennis Shasha: http://www.vldb.org/conf/1994/P439.PDF
//
// The TwoQ struct consists of a master item map, the total capacity in bytes,
// and three basic LRUs. The hot LRU is the "frequently accessed" LRU, which
// contains items that have been requested more than once. The warm LRU is the
// "recently accessed" LRU, which contains items that have been requested only
// once. Items in the hot or warm LRUs should have values that exist in the
// backing cache. When items are evicted from the hot or warm LRUs, they are
// pushed to the front of the cold LRU. Items in the cold LRU represent items
// that have been recently evicted. If an item is inserted into the 2Q LRU that
// exists in the cold LRU, it is immediately added to the front of the hot LRU,
// instead of the warm LRU (where items not yet in any LRU are placed).
//
// When an item is inserted into the 2Q LRU, it checks if it currently exists in
// any of the internal basic LRUs. If in the hot LRU, it is moved to the front.
// If in the warm LRU, the item is removed and inserted into the hot LRU. If in
// in the cold LRU, the item is inserted directly into the hot LRU and the 2Q
// LRU is pruned. If the item does not yet exist in any of the internal LRUs, it
// is inserted into the front of the warm LRU and the 2Q LRU is pruned.
//
// When a pruning occurs, the 2Q LRU's size is compared to its total capacity.
// If the size is less than or equal to the capacity, nothing happens.
// Otherwise, the warm LRU is pruned if its size exceeds its capacity. If the
// warm LRU is under its capacity, the hot LRU is pruned. During pruning, items
// are removed from the back of the LRU and their keys are returned.
type TwoQ struct {
	items    map[string]*listItem // map of all items (hot + warm + cold)
	cap      int64                // total capacity of the LRU in bytes
	pruneCap int64                // total capacity when pruning

	lruHot  *twoQList // LRU for frequently requested items
	lruWarm *twoQList // LRU for items requested only once
	lruCold *twoQList // LRU for recently evicted items
}

// twoQ LRU item statuses
const (
	twoQHot = iota
	twoQWarm
	twoQCold
)

// DefaultTwoQ returns a new TwoQ LRU with the provided capacity.
func DefaultTwoQ(cap int64) *TwoQ {
	return NewTwoQ(cap, 0.001, 0.25, 0.5)
}

// NewTwoQ returns a new TwoQ instance given the provided capacity, eviction
// ratio, warm/hot ratio, and cold ratio.
//
// evictRatio represents the percentage of items (based on size) that should be
// evicted when the LRUs capacity are exceeded.
// warmHotRatio represents the percentage of items (based on size) that should
// exist in the warm LRU compared to the hot LRU. This ratio matters when items
// are being evicted.
// coldRatio is a percentage representing the number of items (based on size)
// that should be kept in the cold LRU compared to the total capacity.
func NewTwoQ(cap int64, evictRatio, warmHotRatio, coldRatio float64) *TwoQ {
	// capacity should be at least 1000 bytes
	if cap < 1000 {
		cap = 1000
	}
	// evict ratio must be between 0.0 & 1.0
	if evictRatio < 0.0 {
		evictRatio = 0.0
	}
	if evictRatio > 1.0 {
		evictRatio = 1.0
	}
	// warm/hot ratio must be between 0.0 & 1.0
	if warmHotRatio < 0.0 {
		warmHotRatio = 0.0
	}
	if warmHotRatio > 1.0 {
		warmHotRatio = 1.0
	}
	// cold ratio must be at least 0.0
	if coldRatio < 0.0 {
		coldRatio = 0.0
	}
	// create 2Q LRU
	pruneCap := int64((1 - evictRatio) * float64(cap))
	coldCap := int64(coldRatio * float64(cap))
	warmCap := int64(warmHotRatio * float64(cap))
	hotCap := cap - warmCap
	tq := &TwoQ{
		items:    make(map[string]*listItem, 10e3),
		cap:      cap,
		pruneCap: pruneCap,
	}
	tq.lruCold = newList(twoQCold, evictRatio, coldCap, tq)
	tq.lruWarm = newList(twoQWarm, evictRatio, warmCap, tq)
	tq.lruHot = newList(twoQHot, evictRatio, hotCap, tq)
	return tq
}

// listItem represents a single item in the LRU.
type listItem struct {
	key    []byte        // the item's key
	status uint8         // the item's status (i.e. hot, warm, cold)
	size   int64         // size of the item's value in bytes
	elem   *list.Element // the item's linked list element
}

// Get returns the size of the value corresponding to the provided key, or -1
// if the key doesn't exist in the LRU.
func (tq *TwoQ) Get(key []byte) int64 {
	if i, ok := tq.items[string(key)]; ok {
		switch i.status {
		case twoQHot:
			// item is in the hot LRU, move it to the front
			tq.lruHot.list.MoveToFront(i.elem)
			return i.size
		case twoQWarm:
			// item is in the warm LRU, move it to the hot LRU
			tq.lruWarm.removeElem(i.elem)
			tq.lruHot.pushToFront(i)
			return i.size
		}
	}
	// the item doesn't exist, return -1
	return -1
}

// PutAndEvict inserts the provided key and value size into the LRU and returns
// a slice of keys that have been evicted and total bytes evicted.
func (tq *TwoQ) PutAndEvict(key []byte, size int64) ([][]byte, int64) {
	keyStr := string(key)
	if i, ok := tq.items[keyStr]; ok {
		i.size = size // update the item's size
		switch i.status {
		case twoQHot:
			// item is already in the hot LRU, move it to the front
			tq.lruHot.list.MoveToFront(i.elem)
			return nil, 0
		case twoQWarm:
			// item is already in the warm LRU, move it to the hot LRU
			tq.lruWarm.removeElem(i.elem)
			tq.lruHot.pushToFront(i)
			return nil, 0
		case twoQCold:
			// item is in the cold LRU, move it to the hot LRU and then prune
			tq.lruCold.removeElem(i.elem)
			tq.lruHot.pushToFront(i)
			return tq.prune()
		}
	}
	// insert the new item into the LRU and then prune it
	i := &listItem{
		key:    key,
		status: twoQWarm,
		size:   size,
	}
	tq.lruWarm.pushToFront(i)
	tq.items[string(i.key)] = i
	return tq.prune()
}

// Cap returns the total capacity of the LRU in bytes.
func (tq *TwoQ) Cap() int64 {
	return tq.cap
}

// Len returns the number of items in the LRU.
func (tq *TwoQ) Len() int64 {
	return int64(tq.lruHot.list.Len() + tq.lruWarm.list.Len())
}

// Size returns the total number of bytes in the LRU.
func (tq *TwoQ) Size() int64 {
	return tq.lruHot.size + tq.lruWarm.size
}

// Empty empties all internal lists.
func (tq *TwoQ) Empty() {
	tq.items = make(map[string]*listItem)
	tq.lruCold.empty()
	tq.lruWarm.empty()
	tq.lruHot.empty()
}

// PutOnStartup adds the provided key and value size into the LRU as an initial
// item. All items are inserted into the warm LRU until full, where items begin
// to be inserted into the cold LRU. It returns true if the item was inserted
// into the warm LRU successfully.
func (tq *TwoQ) PutOnStartup(key []byte, size int64) bool {
	i := &listItem{
		key:  key,
		size: size,
	}
	if tq.Size()+size <= tq.cap {
		tq.lruWarm.pushToFront(i)
		tq.items[string(key)] = i
		return true
	}
	if tq.lruCold.size+size <= tq.lruCold.cap {
		tq.lruCold.pushToFront(i)
		tq.items[string(key)] = i
	}
	return false
}

// prune prunes any excess items off of the back of the warm LRU, or if under
// the warm/hot ratio, the hot LRU, and returns a slice of keys that have been
// evicted and the total bytes evicted.
func (tq *TwoQ) prune() ([][]byte, int64) {
	if tq.Size() <= tq.cap {
		return nil, 0
	}
	eWarm, wbytes := tq.lruWarm.evict()
	eHot, hbytes := tq.lruHot.evict()
	tq.pruneCold()
	return append(eWarm, eHot...), wbytes + hbytes
}

// pruneCold prunes any excess items off of the back of the cold LRU.
func (tq *TwoQ) pruneCold() {
	// ignore pruneCap, prune to its total capacity
	for tq.lruCold.size > tq.lruCold.cap {
		tail := tq.lruCold.list.Back()
		if tail == nil {
			return
		}
		i := tq.lruCold.removeElem(tail)
		delete(tq.items, string(i.key))
	}
}

// twoQList represents a basic LRU.
type twoQList struct {
	list     *list.List // eviction list
	status   uint8      // the list's status (i.e. hot, warm, cold)
	size     int64      // the current size of the list in bytes
	cap      int64      // the list's maximum capacity
	pruneCap int64      // the maximum capacity when pruning
	twoQ     *TwoQ      // the associated TwoQ LRU
}

// newList returns a new twoQList with the provided status, capacity, and twoQ
// LRU.
func newList(status uint8, pruneRatio float64, cap int64, twoQ *TwoQ) *twoQList {
	return &twoQList{
		list:     list.New(),
		status:   status,
		cap:      cap,
		pruneCap: int64((1.0 - pruneRatio) * float64(cap)),
		twoQ:     twoQ,
	}
}

// empty empties the list's underlying linked list and size.
func (ll *twoQList) empty() {
	ll.list = list.New()
	ll.size = 0
}

// pushToFront inserts the provided item into the front of the list.
func (ll *twoQList) pushToFront(i *listItem) {
	i.elem = ll.list.PushFront(i)
	ll.size += i.size
	i.status = ll.status
}

// removeElem removes the provided list element from the linked list and returns
// the associated item.
func (ll *twoQList) removeElem(elem *list.Element) *listItem {
	i := ll.list.Remove(elem).(*listItem)
	ll.size -= i.size
	return i
}

// evict evicts items from the list until the twoQ LRU's size is less than or
// equal to its capacity. It returns a slice of keys that have been evicted and
// the total bytes evicted.
func (ll *twoQList) evict() ([][]byte, int64) {
	var bevicted int64
	var evicted [][]byte
	for ll.twoQ.Size() > ll.twoQ.pruneCap && ll.size > ll.pruneCap {
		tail := ll.list.Back()
		if tail == nil {
			return evicted, bevicted
		}
		i := ll.removeElem(tail)
		ll.twoQ.lruCold.pushToFront(i)
		bevicted += i.size
		evicted = append(evicted, i.key)
	}
	return evicted, bevicted
}
