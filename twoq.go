package lru

import "container/list"

const (
	hot = iota
	warm
	cold
)

type twoQ struct {
	items map[string]*listItem

	lruHot  *lruList
	lruWarm *lruList
	lruCold *lruList
}

func newTwoQ(cap int64, warmHotRatio float64, coldRatio float64) *twoQ {
	if cap < 1000 {
		cap = 1000
	}
	if warmHotRatio < 0.0 {
		warmHotRatio = 0.0
	} else if warmHotRatio > 1.0 {
		warmHotRatio = 1.0
	}
	if coldRatio < 0.0 {
		coldRatio = 0.0
	}
	coldCap := int64(coldRatio * float64(cap))
	warmCap := int64(warmHotRatio * float64(cap))
	hotCap := cap - warmCap
	tq := &twoQ{
		items: make(map[string]*listItem),
	}
	tq.lruCold = newColdList(coldCap, tq.items)
	tq.lruWarm = newWarmList(warmCap, tq.lruCold)
	tq.lruHot = newHotList(hotCap, tq.lruWarm)
	return tq
}

type listItem struct {
	key    []byte
	status uint8
	size   int64
	elem   *list.Element
}

func (tq *twoQ) getAndEvict(key []byte) ([][]byte, int64) {
	if i, ok := tq.items[string(key)]; ok {
		switch i.status {
		case hot:
			tq.lruHot.list.MoveToFront(i.elem)
			return nil, i.size
		case warm:
			return tq.warmToHot(i), i.size
		}
	}
	return nil, -1
}

func (tq *twoQ) putAndEvict(key []byte, size int64) [][]byte {
	keyStr := string(key)
	if i, ok := tq.items[keyStr]; ok {
		i.size = size
		switch i.status {
		case hot:
			tq.lruHot.list.MoveToFront(i.elem)
			return nil
		case warm:
			return tq.warmToHot(i)
		case cold:
			return tq.coldToHot(i)
		}
	}
	i := &listItem{
		key:    key,
		status: warm,
		size:   size,
	}
	return tq.newToWarm(i)
}

func (tq *twoQ) len() int64 {
	return int64(tq.lruHot.list.Len() + tq.lruWarm.list.Len())
}

func (tq *twoQ) size() int64 {
	return tq.lruHot.size + tq.lruWarm.size
}

func (tq *twoQ) empty() {
	tq.items = make(map[string]*listItem)
	tq.lruCold.empty()
	tq.lruWarm.empty()
	tq.lruHot.empty()
}

func (tq *twoQ) addInitialKey(key []byte, size int64) bool {
	i := &listItem{
		key:  key,
		size: size,
	}
	if tq.lruHot.size+size <= tq.lruHot.cap {
		tq.lruHot.pushToFront(i)
		tq.items[string(key)] = i
		return true
	}
	if tq.lruWarm.size+size <= tq.lruWarm.cap {
		tq.lruWarm.pushToFront(i)
		tq.items[string(key)] = i
		return true
	}
	if tq.lruCold.size+size <= tq.lruCold.cap {
		tq.lruCold.pushToFront(i)
	}
	return false
}

/*
   for internal (to this file) use only!
*/
func (tq *twoQ) newToWarm(i *listItem) [][]byte {
	tq.lruWarm.pushToFront(i)
	tq.items[string(i.key)] = i
	return tq.lruWarm.pruneFn()
}

func (tq *twoQ) warmToHot(i *listItem) [][]byte {
	tq.lruWarm.removeElem(i.elem)
	tq.lruHot.pushToFront(i)
	return tq.lruHot.pruneFn()
}

func (tq *twoQ) coldToHot(i *listItem) [][]byte {
	tq.lruCold.removeElem(i.elem)
	tq.lruHot.pushToFront(i)
	return tq.lruHot.pruneFn()
}

type lruList struct {
	list    *list.List
	status  uint8
	size    int64
	cap     int64
	pruneFn func() [][]byte
	next    *lruList
	items   map[string]*listItem
}

func newHotList(cap int64, next *lruList) *lruList {
	ll := &lruList{
		list:   list.New(),
		status: hot,
		cap:    cap,
		next:   next,
	}
	ll.pruneFn = ll.pruneHot
	return ll
}

func newWarmList(cap int64, next *lruList) *lruList {
	ll := &lruList{
		list:   list.New(),
		status: warm,
		cap:    cap,
		next:   next,
	}
	ll.pruneFn = ll.pruneWarm
	return ll
}

func newColdList(cap int64, items map[string]*listItem) *lruList {
	ll := &lruList{
		list:   list.New(),
		status: cold,
		cap:    cap,
		items:  items,
	}
	ll.pruneFn = ll.pruneCold
	return ll
}

func (ll *lruList) empty() {
	ll.list = list.New()
	ll.size = 0

}

func (ll *lruList) pushToFront(i *listItem) {
	i.elem = ll.list.PushFront(i)
	ll.size += i.size
	i.status = ll.status
}

func (ll *lruList) removeElem(elem *list.Element) *listItem {
	i := ll.list.Remove(elem).(*listItem)
	ll.size -= i.size
	return i
}

func (ll *lruList) pruneHot() [][]byte {
	for ll.size > ll.cap {
		tail := ll.list.Back()
		if tail == nil {
			break
		}
		i := ll.removeElem(tail)
		ll.next.pushToFront(i)
	}
	return ll.next.pruneFn()
}

func (ll *lruList) pruneWarm() [][]byte {
	var items [][]byte
	for ll.size > ll.cap {
		tail := ll.list.Back()
		if tail == nil {
			break
		}
		i := ll.removeElem(tail)
		ll.next.pushToFront(i)
		items = append(items, i.key)
	}
	ll.next.pruneFn()
	return items
}

func (ll *lruList) pruneCold() [][]byte {
	for ll.size > ll.cap {
		tail := ll.list.Back()
		if tail == nil {
			return nil
		}
		i := ll.removeElem(tail)
		delete(ll.items, string(i.key))
	}
	return nil
}
