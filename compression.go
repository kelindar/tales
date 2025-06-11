package threads

import (
	"bytes"

	"github.com/kelindar/threads/internal/codec"
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
func compressLogEntries(entries []codec.LogEntry) ([]byte, error) {
	if len(entries) == 0 {
		return nil, nil
	}

	// Concatenate all entries to a single buffer
	buf := &bytes.Buffer{}
	for _, entry := range entries {
		if _, err := buf.Write(entry); err != nil {
			return nil, ErrFormat{Format: "log entries", Err: err}
		}
	}

	// Compress the buffer
	return compressData(buf.Bytes())
}

// decompressLogEntries decompresses log entries.
func decompressLogEntries(compressed []byte) ([]codec.LogEntry, error) {
	if len(compressed) == 0 {
		return nil, nil
	}

	// Decompress the data
	decompressed, err := decompressData(compressed)
	if err != nil {
		return nil, err
	}

	// Parse entries from the decompressed data
	var entries []codec.LogEntry
	buf := decompressed

	for len(buf) > 0 {
		// Parse the current entry to determine its size
		entry := codec.LogEntry(buf)
		sequenceID := entry.ID()
		text := entry.Text()
		actors := entry.Actors()

		// Check if parsing was successful (non-zero sequence ID indicates valid entry)
		if sequenceID == 0 && len(text) == 0 && len(actors) == 0 {
			break // End of valid data
		}

		// Re-encode to get the exact size
		reconstructed, err := codec.NewLogEntry(sequenceID, text, actors)
		if err != nil {
			break
		}

		entrySize := len(reconstructed)
		if len(buf) < entrySize {
			break
		}

		// Extract the entry
		entries = append(entries, codec.LogEntry(buf[:entrySize]))
		buf = buf[entrySize:]
	}

	return entries, nil
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
