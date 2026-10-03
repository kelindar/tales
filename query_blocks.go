package tales

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/kelindar/roaring"
	"github.com/kelindar/tales/internal/codec"
)

// queryPayload coalesces adjacent selected frames into one range request.
func (l *Service) queryPayload(ctx context.Context, payload codec.ObjectRange, blocks []codec.Block, entries uint32, day, from, to time.Time, actors []uint32, writer string, base uint64, selected *roaring.Bitmap, found *eventSelection) error {
	if len(blocks) == 0 {
		blocks = []codec.Block{{Entries: entries, Size: payload.Size}}
	}
	fromMillis, toMillis := queryMillis(day, from, to)
	wanted, matches := selectBlocks(blocks, selected, fromMillis, toMillis)
	writerID, err := strconv.ParseUint(writer, 16, 64)
	if err != nil {
		return fmt.Errorf("invalid writer ID %q", writer)
	}
	found.reserve(matches)
	dayMillis := day.UnixMilli()
	start, stop, step := 0, len(blocks), 1
	if found.limit > 0 && !found.ascending {
		start, stop, step = len(blocks)-1, -1, -1
	}
	for i := start; i != stop; {
		if err := ctx.Err(); err != nil {
			return err
		}
		if wanted[i] == 0 || found.skipBlock(blocks[i], dayMillis, writerID, base) {
			i += step
			continue
		}
		last, count := i, wanted[i]
		need := found.blockLimit(blocks[i], dayMillis, fromMillis, toMillis)
		end := i + step
		for end != stop && wanted[end] > 0 && !found.skipBlock(blocks[end], dayMillis, writerID, base) {
			// Read enough possible matches to fill a page, then reevaluate bounds.
			// Directories without timestamps retain the original coalesced reads.
			if need > 0 && int(count) >= need {
				break
			}
			last = end
			count += wanted[end]
			end += step
		}
		low, high := min(i, last), max(i, last)
		if err := l.readPayload(ctx, payload, blocks[low:high+1], day, from, to, actors, writer, writerID, base, selected, found); err != nil {
			return err
		}
		i = end
	}
	return nil
}

func selectBlocks(blocks []codec.Block, selected *roaring.Bitmap, from, to uint32) ([]uint32, int) {
	wanted := make([]uint32, len(blocks))
	index := 0
	selected.Range(func(ordinal uint32) bool {
		for index+1 < len(blocks) && ordinal >= blocks[index+1].First {
			index++
		}
		wanted[index]++
		return true
	})
	var matches int
	for i, block := range blocks {
		if block.Time != nil && (block.Time[0] > to || block.Time[1] < from) {
			wanted[i] = 0
		}
		matches += int(wanted[i])
	}
	return wanted, matches
}

func (l *Service) readPayload(ctx context.Context, payload codec.ObjectRange, blocks []codec.Block, day, from, to time.Time, actors []uint32, writer string, writerID, base uint64, selected *roaring.Bitmap, found *eventSelection) error {
	last := blocks[len(blocks)-1]
	size := last.Offset + last.Size - blocks[0].Offset
	data, err := l.s3Client.DownloadRange(ctx, payload.Key, payload.ETag, payload.Offset+blocks[0].Offset, size)
	switch {
	case err != nil:
		return err
	case int64(len(data)) != size:
		return fmt.Errorf("read payload blocks: %w", io.ErrUnexpectedEOF)
	}
	for i := range blocks {
		index := i
		if found.limit > 0 && !found.ascending {
			index = len(blocks) - 1 - i
		}
		block := blocks[index]
		if found.skipBlock(block, day.UnixMilli(), writerID, base) {
			continue
		}
		offset := block.Offset - blocks[0].Offset
		raw, err := l.codec.Decompress(data[offset : offset+block.Size])
		if err != nil {
			return fmt.Errorf("decompress payload block: %w", err)
		}
		if err := found.collectFrames(raw, block.Entries, block.First, day, from, to, actors, writer, base, selected); err != nil {
			return err
		}
	}
	return nil
}
