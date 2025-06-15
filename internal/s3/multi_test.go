package s3

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMultiReaderAt(t *testing.T) {
	t.Run("EmptyReader", func(t *testing.T) {
		reader := NewMultiReader()
		assert.Equal(t, int64(0), reader.Size())

		buf := make([]byte, 10)
		n, err := reader.ReadAt(buf, 0)
		assert.Equal(t, 0, n)
		assert.Equal(t, io.EOF, err)
	})

	t.Run("SingleSection", func(t *testing.T) {
		data := []byte("hello world")
		reader := NewMultiReader(data)
		assert.Equal(t, int64(len(data)), reader.Size())

		// Read entire section
		buf := make([]byte, len(data))
		n, err := reader.ReadAt(buf, 0)
		assert.Equal(t, len(data), n)
		assert.NoError(t, err)
		assert.Equal(t, data, buf)

		// Read partial from start
		buf = make([]byte, 5)
		n, err = reader.ReadAt(buf, 0)
		assert.Equal(t, 5, n)
		assert.NoError(t, err)
		assert.Equal(t, []byte("hello"), buf)

		// Read partial from middle
		buf = make([]byte, 5)
		n, err = reader.ReadAt(buf, 6)
		assert.Equal(t, 5, n)
		assert.NoError(t, err)
		assert.Equal(t, []byte("world"), buf)

		// Read past end
		buf = make([]byte, 5)
		n, err = reader.ReadAt(buf, int64(len(data)))
		assert.Equal(t, 0, n)
		assert.Equal(t, io.EOF, err)
	})

	t.Run("MultipleSections", func(t *testing.T) {
		section1 := []byte("hello")
		section2 := []byte(" ")
		section3 := []byte("world")
		reader := NewMultiReader(section1, section2, section3)

		expectedSize := int64(len(section1) + len(section2) + len(section3))
		assert.Equal(t, expectedSize, reader.Size())

		// Read entire content
		buf := make([]byte, expectedSize)
		n, err := reader.ReadAt(buf, 0)
		assert.Equal(t, int(expectedSize), n)
		assert.NoError(t, err)
		assert.Equal(t, []byte("hello world"), buf)

		// Read spanning multiple sections
		buf = make([]byte, 7)
		n, err = reader.ReadAt(buf, 3)
		assert.Equal(t, 7, n)
		assert.NoError(t, err)
		assert.Equal(t, []byte("lo worl"), buf)

		// Read from second section
		buf = make([]byte, 3)
		n, err = reader.ReadAt(buf, 5)
		assert.Equal(t, 3, n)
		assert.NoError(t, err)
		assert.Equal(t, []byte(" wo"), buf)
	})

	t.Run("ReadBeyondEnd", func(t *testing.T) {
		data := []byte("test")
		reader := NewMultiReader(data)

		// Read starting beyond end
		buf := make([]byte, 5)
		n, err := reader.ReadAt(buf, 10)
		assert.Equal(t, 0, n)
		assert.Equal(t, io.EOF, err)

		// Read starting before end but extending beyond
		buf = make([]byte, 10)
		n, err = reader.ReadAt(buf, 2)
		assert.Equal(t, 2, n) // Only "st" from "test"
		assert.NoError(t, err)
		assert.Equal(t, []byte("st"), buf[:n])
	})

	t.Run("NegativeOffset", func(t *testing.T) {
		data := []byte("test")
		reader := NewMultiReader(data)

		buf := make([]byte, 5)
		n, err := reader.ReadAt(buf, -1)
		assert.Equal(t, 0, n)
		assert.Equal(t, io.ErrUnexpectedEOF, err)
	})

	t.Run("EmptySections", func(t *testing.T) {
		section1 := []byte("hello")
		section2 := []byte{} // Empty section
		section3 := []byte("world")
		reader := NewMultiReader(section1, section2, section3)

		expectedSize := int64(len(section1) + len(section3))
		assert.Equal(t, expectedSize, reader.Size())

		// Read entire content (should skip empty section)
		buf := make([]byte, expectedSize)
		n, err := reader.ReadAt(buf, 0)
		assert.Equal(t, int(expectedSize), n)
		assert.NoError(t, err)
		assert.Equal(t, []byte("helloworld"), buf)
	})

	t.Run("LargeRead", func(t *testing.T) {
		// Test with larger data to ensure efficiency
		section1 := make([]byte, 1000)
		section2 := make([]byte, 2000)
		section3 := make([]byte, 1500)

		// Fill with test data
		for i := range section1 {
			section1[i] = byte(i % 256)
		}
		for i := range section2 {
			section2[i] = byte((i + 1000) % 256)
		}
		for i := range section3 {
			section3[i] = byte((i + 3000) % 256)
		}

		reader := NewMultiReader(section1, section2, section3)
		expectedSize := int64(4500)
		assert.Equal(t, expectedSize, reader.Size())

		// Read a chunk spanning all sections
		buf := make([]byte, 4500)
		n, err := reader.ReadAt(buf, 0)
		assert.Equal(t, 4500, n)
		assert.NoError(t, err)

		// Verify content
		assert.Equal(t, section1, buf[0:1000])
		assert.Equal(t, section2, buf[1000:3000])
		assert.Equal(t, section3, buf[3000:4500])
	})

	t.Run("SmallBufferReads", func(t *testing.T) {
		section1 := []byte("abc")
		section2 := []byte("def")
		section3 := []byte("ghi")
		reader := NewMultiReader(section1, section2, section3)

		// Read with very small buffer
		buf := make([]byte, 1)
		result := make([]byte, 0, 9)

		for offset := int64(0); offset < reader.Size(); offset++ {
			n, err := reader.ReadAt(buf, offset)
			assert.Equal(t, 1, n)
			assert.NoError(t, err)
			result = append(result, buf[0])
		}

		assert.Equal(t, []byte("abcdefghi"), result)
	})

	t.Run("ReadInterface", func(t *testing.T) {
		data := []byte("hello world")
		reader := NewMultiReader(data)

		// Test Read method (reads from beginning)
		buf := make([]byte, 5)
		n, err := reader.Read(buf)
		assert.Equal(t, 5, n)
		assert.NoError(t, err)
		assert.Equal(t, []byte("hello"), buf)
	})

	t.Run("OffsetCalculation", func(t *testing.T) {
		// Test that offsets are calculated correctly
		section1 := []byte("12345")    // offset 0, size 5
		section2 := []byte("abcdefgh") // offset 5, size 8
		section3 := []byte("xyz")      // offset 13, size 3
		reader := NewMultiReader(section1, section2, section3)

		// Read from exact section boundaries
		buf := make([]byte, 1)

		// First byte of section1
		n, err := reader.ReadAt(buf, 0)
		assert.Equal(t, 1, n)
		assert.NoError(t, err)
		assert.Equal(t, []byte("1"), buf)

		// First byte of section2
		n, err = reader.ReadAt(buf, 5)
		assert.Equal(t, 1, n)
		assert.NoError(t, err)
		assert.Equal(t, []byte("a"), buf)

		// First byte of section3
		n, err = reader.ReadAt(buf, 13)
		assert.Equal(t, 1, n)
		assert.NoError(t, err)
		assert.Equal(t, []byte("x"), buf)

		// Last byte
		n, err = reader.ReadAt(buf, 15)
		assert.Equal(t, 1, n)
		assert.NoError(t, err)
		assert.Equal(t, []byte("z"), buf)
	})

	t.Run("CrossSectionReads", func(t *testing.T) {
		section1 := []byte("abc")
		section2 := []byte("def")
		section3 := []byte("ghi")
		reader := NewMultiReader(section1, section2, section3)

		// Read spanning section1 and section2
		buf := make([]byte, 4)
		n, err := reader.ReadAt(buf, 2)
		assert.Equal(t, 4, n)
		assert.NoError(t, err)
		assert.Equal(t, []byte("cdef"), buf)

		// Read spanning all three sections
		buf = make([]byte, 5)
		n, err = reader.ReadAt(buf, 2)
		assert.Equal(t, 5, n)
		assert.NoError(t, err)
		assert.Equal(t, []byte("cdefg"), buf)

		// Read spanning section2 and section3
		buf = make([]byte, 4)
		n, err = reader.ReadAt(buf, 5)
		assert.Equal(t, 4, n)
		assert.NoError(t, err)
		assert.Equal(t, []byte("fghi"), buf)
	})
}
