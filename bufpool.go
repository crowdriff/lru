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

// writerTo represents a struct that contains an optional buffer and data. It
// purposely conforms to the io.WriterTo interface. After WriteTo is called, the
//  writerTo's buffer (if non-nil) is put back into the buffer pool.
type writerTo struct {
	written bool
	buf     *bytes.Buffer
	data    []byte
}

// newWriterToFromBuf returns a new writerTo pointer from the provided buffer.
func newWriterToFromBuf(buf *bytes.Buffer) *writerTo {
	return &writerTo{buf: buf, data: buf.Bytes()}
}

// newWriterToFromData returns a new writerTo pointer from the provided byte
// slice.
func newWriterToFromData(data []byte) *writerTo {
	return &writerTo{data: data}
}

// WriteTo writes the data from the writerTo to the provided io.Writer. WriteTo
// should only be called once, subsequent calls will return a bytes written of
// 0. After finishing the Write, the buffer is put back into the buffer pool
// (if non-nil).
func (wt *writerTo) WriteTo(w io.Writer) (int64, error) {
	if wt.written {
		return 0, nil
	}
	wt.written = true
	n, err := w.Write(wt.data)
	if wt.buf != nil {
		putBuf(wt.buf)
		wt.buf = nil
	}
	wt.data = nil
	return int64(n), err
}
