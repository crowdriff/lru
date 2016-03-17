package lru

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Bufpool", func() {

	Context("WriteTo", func() {

		It("should write to the provided io.Writer", func() {
			wt := newWriterToFromData([]byte("test data"))
			Ω(wt).ShouldNot(BeNil())
			testWriteTo(wt)
		})

		It("should return 0 bytes written on subsequent calls to WriteTo", func() {
			wt := newWriterToFromData([]byte("test data"))
			Ω(wt).ShouldNot(BeNil())
			testWriteTo(wt)
			buf := getBuf()
			defer putBuf(buf)
			n, err := wt.WriteTo(buf)
			Ω(err).ShouldNot(HaveOccurred())
			Ω(n).Should(Equal(int64(0)))
			Ω(buf.Len()).Should(Equal(0))
		})
	})
})

func testWriteTo(wt *writerTo) {
	buf := getBuf()
	defer putBuf(buf)
	n, err := wt.WriteTo(buf)
	Ω(err).ShouldNot(HaveOccurred())
	Ω(n).Should(Equal(int64(9)))
	Ω(wt.buf).Should(BeNil())
	Ω(wt.data).Should(BeNil())
	Ω(wt.written).Should(BeTrue())
	Ω(buf.String()).Should(Equal("test data"))
}
