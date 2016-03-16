package lru

import (
	"errors"
)

// Store is an interface representing a remote data store.
type Store interface {
	Get([]byte) ([]byte, error) // retrieve the value with the provided key
	Open() error                // open the store
	Close() error               // close the store
}

// errNoStore is the error returned by a "noStore" store if the Get method is
// called on it.
var errNoStore = errors.New("no remote store available")

// noStore conforms to the Store interface and returns an error upon any Get
// evocation.
type noStore struct{}

func (s *noStore) Get(key []byte) ([]byte, error) {
	return nil, errNoStore
}

func (s *noStore) Open() error {
	return nil
}

func (s *noStore) Close() error {
	return nil
}
