// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package codec

import (
	"github.com/klauspost/compress/zstd"
)

// Codec handles ZSTD compression and decompression operations.
type Codec struct {
	encoder *zstd.Encoder
	decoder *zstd.Decoder
}

// NewCodec creates a new codec with ZSTD encoder and decoder.
func NewCodec() (*Codec, error) {
	// Create encoder with default settings
	encoder, err := zstd.NewWriter(nil)
	if err != nil {
		return nil, &CompressionError{Operation: "create encoder", Err: err}
	}

	// Create decoder with default settings
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		encoder.Close() // Clean up encoder if decoder creation fails
		return nil, &CompressionError{Operation: "create decoder", Err: err}
	}

	return &Codec{
		encoder: encoder,
		decoder: decoder,
	}, nil
}

// Compress compresses data using ZSTD compression.
func (c *Codec) Compress(data []byte) ([]byte, error) {
	compressed := c.encoder.EncodeAll(data, make([]byte, 0, len(data)))
	return compressed, nil
}

// Decompress decompresses ZSTD compressed data.
func (c *Codec) Decompress(compressed []byte) ([]byte, error) {
	decompressed, err := c.decoder.DecodeAll(compressed, nil)
	if err != nil {
		return nil, &CompressionError{Operation: "decompress", Err: err}
	}

	return decompressed, nil
}

// Close closes the encoder and decoder.
func (c *Codec) Close() {
	if c.encoder != nil {
		c.encoder.Close()
		c.encoder = nil
	}
	if c.decoder != nil {
		c.decoder.Close()
		c.decoder = nil
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
