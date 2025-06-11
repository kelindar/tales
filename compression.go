package threads

import (
	"bytes"

	"github.com/klauspost/compress/zstd"
)

// Compression utilities for ZSTD compression/decompression.

var (
	// Global encoder and decoder for reuse
	encoder *zstd.Encoder
	decoder *zstd.Decoder
)

// initCompression initializes the global encoder and decoder.
func initCompression() error {
	var err error

	// Create encoder with default settings
	encoder, err = zstd.NewWriter(nil)
	if err != nil {
		return ErrCompression{Operation: "create encoder", Err: err}
	}

	// Create decoder with default settings
	decoder, err = zstd.NewReader(nil)
	if err != nil {
		return ErrCompression{Operation: "create decoder", Err: err}
	}

	return nil
}

// compressData compresses data using ZSTD compression.
func compressData(data []byte) ([]byte, error) {
	if encoder == nil {
		if err := initCompression(); err != nil {
			return nil, err
		}
	}

	compressed := encoder.EncodeAll(data, make([]byte, 0, len(data)))
	return compressed, nil
}

// decompressData decompresses ZSTD compressed data.
func decompressData(compressed []byte) ([]byte, error) {
	if decoder == nil {
		if err := initCompression(); err != nil {
			return nil, err
		}
	}

	decompressed, err := decoder.DecodeAll(compressed, nil)
	if err != nil {
		return nil, ErrCompression{Operation: "decompress", Err: err}
	}

	return decompressed, nil
}

// compressLogEntries compresses a slice of log entries.
func compressLogEntries(entries []*LogEntry) ([]byte, error) {
	if len(entries) == 0 {
		return nil, nil
	}

	// Encode all entries to a single buffer
	buf := &bytes.Buffer{}
	for _, entry := range entries {
		encoded, err := encodeLogEntry(entry)
		if err != nil {
			return nil, err
		}
		if _, err := buf.Write(encoded); err != nil {
			return nil, ErrFormat{Format: "log entries", Err: err}
		}
	}

	// Compress the buffer
	return compressData(buf.Bytes())
}

// decompressLogEntries decompresses and decodes log entries.
func decompressLogEntries(compressed []byte) ([]*LogEntry, error) {
	if len(compressed) == 0 {
		return nil, nil
	}

	// Decompress the data
	decompressed, err := decompressData(compressed)
	if err != nil {
		return nil, err
	}

	// Decode entries from the decompressed data
	var entries []*LogEntry
	buf := bytes.NewReader(decompressed)

	for buf.Len() > 0 {
		// Read the next entry
		// We need to determine the entry size by parsing it
		remaining := make([]byte, buf.Len())
		if _, err := buf.Read(remaining); err != nil {
			return nil, ErrFormat{Format: "log entries", Err: err}
		}

		// Reset buffer to beginning of remaining data
		buf = bytes.NewReader(remaining)

		// Try to decode one entry
		entry, err := decodeLogEntry(remaining)
		if err != nil {
			return nil, err
		}

		entries = append(entries, entry)

		// Calculate how much data was consumed
		consumed := calculateLogEntrySize(entry)
		if consumed > len(remaining) {
			break
		}

		// Move to next entry
		buf = bytes.NewReader(remaining[consumed:])
	}

	return entries, nil
}

// calculateLogEntrySize calculates the binary size of a log entry.
func calculateLogEntrySize(entry *LogEntry) int {
	size := 4 // sequence ID
	size += varintSize(uint64(len(entry.Text)))
	size += varintSize(uint64(len(entry.Actors)))
	size += len(entry.Text)
	size += len(entry.Actors) * 4 // 4 bytes per actor ID
	return size
}

// varintSize calculates the size of a varint encoding.
func varintSize(value uint64) int {
	size := 1
	for value >= 0x80 {
		size++
		value >>= 7
	}
	return size
}

// compressBitmap compresses a roaring bitmap.
func compressBitmap(bitmapData []byte) ([]byte, error) {
	return compressData(bitmapData)
}

// decompressBitmap decompresses a roaring bitmap.
func decompressBitmap(compressed []byte) ([]byte, error) {
	return decompressData(compressed)
}

// closeCompression closes the global encoder and decoder.
func closeCompression() {
	if encoder != nil {
		encoder.Close()
		encoder = nil
	}
	if decoder != nil {
		decoder.Close()
		decoder = nil
	}
}
