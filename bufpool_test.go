package lru

import (
	"bytes"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Bufpool", func() {

	Context("Buffer", func() {

		It("should create a new Buffer using a bytes.Buffer", func() {
			buf := &bytes.Buffer{}
			buf.WriteString("test")
			b := newBufferFromBuf(buf)
			Ω(b).ShouldNot(BeNil())
			Ω(b.buf).Should(Equal(buf))
			Ω(b.closed).Should(BeFalse())
			Ω(string(b.data)).Should(Equal("test"))
		})

		It("should create a new Buffer using a byte slice", func() {
			b := newBufferFromData([]byte("test"))
			Ω(b).ShouldNot(BeNil())
			Ω(b.buf).Should(BeNil())
			Ω(b.closed).Should(BeFalse())
			Ω(string(b.data)).Should(Equal("test"))
		})

		It("should return the underlying bytes of the buffer", func() {
			b := newBufferFromData([]byte("test"))
			Ω(string(b.Bytes())).Should(Equal("test"))
		})

		It("should return nil when closing an already closed Buffer", func() {
			b := newBufferFromData([]byte("test"))
			b.closed = true
			err := b.Close()
			Ω(err).ShouldNot(HaveOccurred())
		})

		It("should return nil and release the underlying buffer when closed", func() {
			buf := &bytes.Buffer{}
			buf.WriteString("test")
			b := newBufferFromBuf(buf)
			err := b.Close()
			Ω(err).ShouldNot(HaveOccurred())
			Ω(b.buf).Should(BeNil())
			Ω(b.data).Should(BeNil())
			Ω(b.closed).Should(BeTrue())
		})

		It("should write to the provided io.Writer", func() {
			b := newBufferFromData([]byte("test"))
			buf := &bytes.Buffer{}
			n, err := b.WriteTo(buf)
			Ω(err).ShouldNot(HaveOccurred())
			Ω(n).Should(Equal(int64(4)))
		})

		It("should write no bytes if the Buffer is closed", func() {
			b := newBufferFromData([]byte("test"))
			b.closed = true
			buf := &bytes.Buffer{}
			n, err := b.WriteTo(buf)
			Ω(err).ShouldNot(HaveOccurred())
			Ω(n).Should(Equal(int64(0)))
		})
	})
})
