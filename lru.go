package lru

import (
	"container/list"
	"sync"
	"time"

	"github.com/boltdb/bolt"
)

// LRU represents a read-through LRU cache backed by a persistent boltDB
// database.
type LRU struct {
	// PostStoreFn is an optional function that is called after a response has
	// been received from the remote store. Its two arguments are the item's
	// value and the error returned by the remote store's Get method. It should
	// return the value and error that will be stored in the LRU and returned
	// to the calling Get method. This function should not be updated
	// concurrently while using the LRU.
	PostStoreFn func([]byte, error) ([]byte, error)

	// boltDB cache
	cache  *bolt.DB
	dbPath string
	bName  []byte // LRU bucket name

	// remote store
	store  Store
	muReqs sync.Mutex      // mutex protecting the reqs map
	reqs   map[string]*req // map of current remote store requests

	// cache capacity and prune capacity
	cap      int64 // maximum capacity in bytes
	prunecap int64 // minimum bytes to prune when evicting items (0.1% of capacity)

	// mutex protecting everything below
	mu sync.Mutex

	// internal cache objects
	remain int64            // space remaining in bytes
	items  map[string]*item // map of all items
	list   *list.List       // eviction linked list

	// cache stats
	sTime   time.Time // starting time
	hits    int64     // # of cache hits
	misses  int64     // # of cache misses
	bget    int64     // # of bytes retrieved
	puts    int64     // # of puts completed
	bput    int64     // # of bytes written
	evicted int64     // # of items evicted
	bevict  int64     // # of bytes evicted
}

// item represents a single item in the eviction list.
type item struct {
	key  []byte        // item's key
	size int64         // size of the item's value in bytes
	elem *list.Element // linked list pointer
}

// req represents a remote store request.
type req struct {
	wg    sync.WaitGroup
	value []byte
	err   error
}

// NewLRU returns a new LRU object with the provided capacity, database path,
// db bucket name, and remote store. Before using the returned LRU, its Open
// method must be called first.
func NewLRU(cap int64, dbPath, bName string, store Store) *LRU {
	// mimimum capacity is 1000 bytes
	if cap < 1000 {
		cap = 1000
	}
	// assign a default database path of "/tmp/lru.db"
	if dbPath == "" {
		dbPath = "/tmp/lru.db"
	}
	// assign a default bucket name of "lru"
	if bName == "" {
		bName = "lru"
	}
	// assign nostore if no store is provided
	if store == nil {
		store = &noStore{}
	}
	// initialize LRU
	return &LRU{
		dbPath:   dbPath,
		bName:    []byte(bName),
		store:    store,
		reqs:     make(map[string]*req),
		cap:      cap,
		prunecap: int64(0.001 * float64(cap)),
		remain:   cap,
		items:    make(map[string]*item, 10e3),
		list:     list.New(),
		sTime:    time.Now().UTC(),
	}
}

// Open opens the LRU's remote store and, if successful, the local bolt
// database. If the bolt database contains existing items, the cache is filled
// up to its capacity and the overflow is deleted.
func (l *LRU) Open() error {
	if err := l.store.Open(); err != nil {
		return err
	}
	return l.openBoltDB()
}

// Close closes the LRU's remote store and the connection to the local bolt
// database and returns any error encountered.
func (l *LRU) Close() error {
	if err := l.store.Close(); err != nil {
		l.close()
		return err
	}
	return l.close()
}

// close closes the underlying bolt database and zeros the LRU. An LRU cannot
// be used after calling this method.
func (l *LRU) close() error {
	l.mu.Lock()
	l.list = list.New()
	l.items = make(map[string]*item)
	l.remain = l.cap
	l.mu.Unlock()
	return l.cache.Close()
}

// Get attempts to retrieve the value for the provided key. An error is returned
// if either no value exists or an error occurs retrieving the value from the
// remote store.
func (l *LRU) Get(key []byte) ([]byte, error) {
	// attempt to get from local cache
	if l.hit(key) {
		if v := l.getFromBolt(key); v != nil {
			return v, nil
		}
	}
	// retrieve from the remote store
	return l.getFromStore(key)
}

// Empty completely empties the cache and underlying bolt database.
func (l *LRU) Empty() error {
	l.mu.Lock()
	l.items = make(map[string]*item)
	l.list = list.New()
	l.remain = l.cap
	l.mu.Unlock()
	return l.emptyBolt()
}

// hit registers a 'hit' for the provided key in the LRU and returns true if
// the key exists.
func (l *LRU) hit(key []byte) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if item, ok := l.items[string(key)]; ok {
		l.list.MoveToFront(item.elem)
		l.hits++
		l.bget += item.size
		return true
	}
	l.misses++
	return false
}

// getFromStore attempts to retrieve the value with the provided key from the
// remote store. If another goroutine has already requested the same value,
// this method will wait for that request to complete and return the resulting
// value and error.
func (l *LRU) getFromStore(key []byte) ([]byte, error) {
	keyStr := string(key)

	// register request
	l.muReqs.Lock()
	if r, ok := l.reqs[keyStr]; ok {
		// a request for this key is in currently in progress
		l.muReqs.Unlock()
		r.wg.Wait()
		return r.value, r.err
	}
	r := &req{}
	r.wg.Add(1)
	l.reqs[keyStr] = r
	l.muReqs.Unlock()

	// obtain from the remote store and call the PostStoreFn if non-nil
	r.value, r.err = l.store.Get(key)
	if l.PostStoreFn != nil {
		r.value, r.err = l.PostStoreFn(r.value, r.err)
	}
	r.wg.Done()

	// if an error occurred, delete the request and return the error.
	if r.err != nil {
		l.deleteReq(keyStr)
		return nil, r.err
	}

	// in a new goroutine, write the received value to the cache and then delete
	// the request from the "reqs" map.
	go func() {
		l.put(key, r.value)
		l.deleteReq(keyStr)
	}()

	return r.value, nil
}

// deleteReq safely deletes the request from the "reqs" map with the provided
// key.
func (l *LRU) deleteReq(key string) {
	l.muReqs.Lock()
	delete(l.reqs, key)
	l.muReqs.Unlock()
}

// put adds the provided key and value to the local cache and LRU. If the cache
// exceeds its capacity, the least recently used item(s) will be evicted.
func (l *LRU) put(key, val []byte) error {
	// add to boltdb store
	err := l.putIntoBolt(key, val)
	if err != nil {
		return err
	}
	// add to LRU
	l.addItem(key, int64(len(val)))
	return nil
}

// addItem adds the provided key and size to the LRU. If there are any items
// that have been pruned, they will be deleted from the bolt database in a new
// goroutine.
func (l *LRU) addItem(key []byte, size int64) {
	l.mu.Lock()
	toPrune := l.addItemWithMu(key, size)
	l.puts++
	l.bput += size
	l.mu.Unlock()
	if len(toPrune) > 0 {
		go l.deleteFromBolt(toPrune)
	}
}

// addItemWithMu adds the provided key and size to the LRU and calls "prune" if
// the LRU has exceeded its capacity. If there are any items that have been
// pruned from the LRU (but not the bolt database, yet), their keys are
// returned.
// Note: this method should only be called when the LRUs mutex is locked!
func (l *LRU) addItemWithMu(key []byte, size int64) [][]byte {
	l.remain -= size
	keyStr := string(key)
	item := item{
		key:  key,
		size: size,
	}
	item.elem = l.list.PushFront(&item)
	l.items[keyStr] = &item
	if l.remain < 0 {
		return l.prune()
	}
	return nil
}

// prune evicts the least recently used items from the LRU until the prune
// capacity has been reached. The keys of the pruned items are returned.
// Note: this method should only be called when the LRUs mutex is locked!
func (l *LRU) prune() [][]byte {
	var toPrune [][]byte
	for l.remain < l.prunecap {
		tail := l.list.Back()
		if tail == nil {
			return toPrune
		}
		item := l.list.Remove(tail).(*item)
		delete(l.items, string(item.key))
		toPrune = append(toPrune, item.key)
		l.remain += item.size
		l.evicted++
		l.bevict += item.size
	}
	return toPrune
}
