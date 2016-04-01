package lru

import (
	"bytes"
	"io"
	"sync"
)

// bufpool is a global pool of buffers for use within the LRU.
var bufpool = &sync.Pool{
	New: func() interface{} {
		return &bytes.Buffer{}
	},
}

// getBuf retrieves a buffer from the pool.
func getBuf() *bytes.Buffer {
	return bufpool.Get().(*bytes.Buffer)
}

// putBuf puts the provided buffer back into the pool.
func putBuf(buf *bytes.Buffer) {
	buf.Reset()
	bufpool.Put(buf)
}

// Buffer represents data obtained from the cache using an underlying pooled
// buffer. After using a Buffer, its Close method should be called to release
// the underlying buffer back into the global pool. At this time, the byte slice
// returned from the Bytes method is invalid, and should not be used any
// further.
type Buffer struct {
	closed bool
	buf    *bytes.Buffer
	data   []byte
}

// newBufferFromBuf returns a new Buffer from the provided bytes.Buffer.
func newBufferFromBuf(buf *bytes.Buffer) *Buffer {
	return &Buffer{buf: buf, data: buf.Bytes()}
}

// newBufferFromData returns a new Buffer from the provided byte slice.
func newBufferFromData(data []byte) *Buffer {
	return &Buffer{data: data}
}

// Bytes returns the Buffer's underlying byte slice. The returned slice is only
// valid before calling the Buffer's Close method. After that time, its contents
// may change or become invalid.
func (b *Buffer) Bytes() []byte {
	return b.data
}

// Close puts the underlying buffer back into the shared pool. The returned
// error is always nil.
func (b *Buffer) Close() error {
	if b.closed {
		return nil
	}
	b.closed = true
	if b.buf != nil {
		putBuf(b.buf)
		b.buf = nil
	}
	b.data = nil
	return nil
}

// WriteTo writes the Buffer's contents to the provided io.Writer and returns
// the number of bytes written and any error encountered.
func (b *Buffer) WriteTo(w io.Writer) (int64, error) {
	if b.closed {
		return 0, nil
	}
	n, err := w.Write(b.data)
	return int64(n), err
}
