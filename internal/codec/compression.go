package codec

import (
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
		return &CompressionError{Operation: "create encoder", Err: err}
	}

	// Create decoder with default settings
	decoder, err = zstd.NewReader(nil)
	if err != nil {
		return &CompressionError{Operation: "create decoder", Err: err}
	}

	return nil
}

// Compress compresses data using ZSTD compression.
func Compress(data []byte) ([]byte, error) {
	if encoder == nil {
		if err := initCompression(); err != nil {
			return nil, err
		}
	}

	compressed := encoder.EncodeAll(data, make([]byte, 0, len(data)))
	return compressed, nil
}

// Decompress decompresses ZSTD compressed data.
func Decompress(compressed []byte) ([]byte, error) {
	if decoder == nil {
		if err := initCompression(); err != nil {
			return nil, err
		}
	}

	decompressed, err := decoder.DecodeAll(compressed, nil)
	if err != nil {
		return nil, &CompressionError{Operation: "decompress", Err: err}
	}

	return decompressed, nil
}

// Close closes the global encoder and decoder.
func Close() {
	if encoder != nil {
		encoder.Close()
		encoder = nil
	}
	if decoder != nil {
		decoder.Close()
		decoder = nil
	}
}

// CompressionError represents a compression/decompression error.
type CompressionError struct {
	Operation string
	Err       error
}

func (e *CompressionError) Error() string {
	return "compression operation failed (" + e.Operation + "): " + e.Err.Error()
}

func (e *CompressionError) Unwrap() error {
	return e.Err
}
