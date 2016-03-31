package lru

// Algorithm represents the underlying algorithm managing an LRU.
type Algorithm interface {
	// Cap returns the total capacity of the LRU in bytes.
	Cap() int64

	// Empty completely empties the LRU.
	Empty()

	// Get returns the size of the item identified by the provided key, or
	// -1 if the key does not exist in the LRU.
	Get([]byte) int64

	// Len returns the total number of items in the LRU.
	Len() int64

	// PutAndEvict inserts the provided key and size into the LRU and
	// returns a slice of keys that have been evicted as well as the total
	// size in bytes that were evicted.
	PutAndEvict([]byte, int64) ([][]byte, int64)

	// PutOnStartup adds the provided key and size to LRU and returns true
	// if the key was successfully added.
	PutOnStartup([]byte, int64) bool

	// Size returns the total size in bytes of all items in the LRU.
	Size() int64
}
