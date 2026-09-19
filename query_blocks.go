package tales

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/kelindar/roaring"
	"github.com/kelindar/tales/internal/codec"
)

// queryPayload coalesces adjacent selected frames into one range request.
func (l *Service) queryPayload(ctx context.Context, payload codec.ObjectRange, blocks []codec.Block, entries uint32, day, from, to time.Time, actors []uint32, writer string, base uint64, selected *roaring.Bitmap) ([]eventRef, error) {
	if len(blocks) == 0 {
		blocks = []codec.Block{{Entries: entries, Size: payload.Size}}
	}
	wanted := make([]bool, len(blocks))
	index := 0
	selected.Range(func(ordinal uint32) bool {
		for index+1 < len(blocks) && ordinal >= blocks[index+1].First {
			index++
		}
		wanted[index] = true
		return true
	})
	var refs []eventRef
	for i := 0; i < len(blocks); {
		if !wanted[i] {
			i++
			continue
		}
		end := i + 1
		for end < len(blocks) && wanted[end] {
			end++
		}
		last := blocks[end-1]
		data, err := l.s3Client.DownloadRange(ctx, payload.Key, payload.ETag, payload.Offset+blocks[i].Offset, last.Offset+last.Size-blocks[i].Offset)
		switch {
		case err != nil:
			return nil, err
		case int64(len(data)) != last.Offset+last.Size-blocks[i].Offset:
			return nil, fmt.Errorf("read payload blocks: %w", io.ErrUnexpectedEOF)
		}
		for _, block := range blocks[i:end] {
			offset := block.Offset - blocks[i].Offset
			raw, err := l.codec.Decompress(data[offset : offset+block.Size])
			if err != nil {
				return nil, fmt.Errorf("decompress payload block: %w", err)
			}
			found, err := collectFrames(raw, block.Entries, block.First, day, from, to, actors, writer, base, selected)
			if err != nil {
				return nil, err
			}
			refs = append(refs, found...)
		}
		i = end
	}
	return refs, nil
}
