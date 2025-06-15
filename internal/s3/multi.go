package s3

import (
	"io"
)

// MultiReader implements io.ReaderAt for multiple byte slices concatenated together.
// This avoids expensive memory allocations and copies when combining multiple data sources.
type MultiReader struct {
	sections [][]byte
	offsets  []int64
	size     int64
}

// NewMultiReader creates a new MultiReaderAt from multiple byte slices.
func NewMultiReader(sections ...[]byte) *MultiReader {
	offsets := make([]int64, len(sections))
	var totalSize int64

	for i, section := range sections {
		offsets[i] = totalSize
		totalSize += int64(len(section))
	}

	return &MultiReader{
		sections: sections,
		offsets:  offsets,
		size:     totalSize,
	}
}

// ReadAt implements io.ReaderAt interface.
func (m *MultiReader) ReadAt(p []byte, off int64) (n int, err error) {
	if off < 0 {
		return 0, io.ErrUnexpectedEOF
	}
	if off >= m.size {
		return 0, io.EOF
	}

	// Find which section contains the starting offset
	sectionIdx := 0
	for i := range m.offsets {
		if i+1 < len(m.offsets) && off >= m.offsets[i+1] {
			continue
		}
		sectionIdx = i
		break
	}

	totalRead := 0
	currentOffset := off

	// Read from sections starting at sectionIdx
	for sectionIdx < len(m.sections) && totalRead < len(p) {
		section := m.sections[sectionIdx]
		sectionStart := m.offsets[sectionIdx]
		offsetInSection := currentOffset - sectionStart

		// Skip if we're past this section
		if offsetInSection >= int64(len(section)) {
			sectionIdx++
			if sectionIdx < len(m.offsets) {
				currentOffset = m.offsets[sectionIdx] // Move to start of next section
			}
			continue
		}

		// Calculate how much to read from this section
		remainingInSection := int64(len(section)) - offsetInSection
		remainingInBuffer := int64(len(p)) - int64(totalRead)
		toRead := remainingInSection
		if toRead > remainingInBuffer {
			toRead = remainingInBuffer
		}

		// Copy data from this section
		copy(p[totalRead:totalRead+int(toRead)], section[offsetInSection:offsetInSection+toRead])
		totalRead += int(toRead)
		currentOffset += toRead

		// Move to next section
		sectionIdx++
		if sectionIdx < len(m.offsets) {
			currentOffset = m.offsets[sectionIdx]
		}
	}

	if totalRead == 0 && off < m.size {
		return 0, io.ErrUnexpectedEOF
	}

	return totalRead, nil
}

// Read implements io.Reader interface for compatibility.
func (m *MultiReader) Read(p []byte) (n int, err error) {
	// This is a simple implementation that reads from the beginning
	// For a full implementation, we'd need to track current position
	return m.ReadAt(p, 0)
}

// Size returns the total size of all sections combined.
func (m *MultiReader) Size() int64 {
	return m.size
}
