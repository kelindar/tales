package buffer

import (
	"fmt"
	"sort"
	"time"

	"github.com/kelindar/roaring"
	"github.com/kelindar/tales/internal/codec"
)

type Buffer struct {
	codec *codec.Codec
	data  []byte
	index map[uint32]*roaring.Bitmap
	count int
	max   int
	day   time.Time
	time  [2]uint32
}

func New(maxEntries int, c *codec.Codec) *Buffer {
	return &Buffer{codec: c, data: make([]byte, 0, 8<<20), index: make(map[uint32]*roaring.Bitmap), max: maxEntries}
}

func (b *Buffer) Size() int      { return b.count }
func (b *Buffer) Bytes() int     { return len(b.data) }
func (b *Buffer) Day() time.Time { return b.day }

func (b *Buffer) Add(day time.Time, entry codec.LogEntry) error {
	if b.count >= b.max {
		return fmt.Errorf("event buffer full")
	}
	if b.count > 0 && !b.day.Equal(day) {
		return fmt.Errorf("event day changed")
	}
	if b.count == 0 {
		b.day = day
		b.time = [2]uint32{entry.Millis(), entry.Millis()}
	} else {
		b.time[0] = min(b.time[0], entry.Millis())
		b.time[1] = max(b.time[1], entry.Millis())
	}
	ordinal := uint32(b.count)
	b.data = append(b.data, entry...)
	b.count++
	for actor := range entry.Actors() {
		bm := b.index[actor]
		if bm == nil {
			bm = roaring.New()
			b.index[actor] = bm
		}
		bm.Set(ordinal)
	}
	return nil
}

type Binary struct {
	Compressed []byte
	RawSize    int64
}

type Index struct {
	Actor uint32
	Data  []byte
}

type Batch struct {
	Day     time.Time
	Raw     []byte
	Data    Binary
	Indexes []Index
	Entries uint32
	Time    [2]uint32
}

func (b *Buffer) Take() (*Batch, error) {
	if b.count == 0 {
		return nil, nil
	}
	compressed, err := b.codec.Compress(b.data)
	if err != nil {
		return nil, err
	}
	actors := make([]uint32, 0, len(b.index))
	for actor := range b.index {
		actors = append(actors, actor)
	}
	sort.Slice(actors, func(i, j int) bool { return actors[i] < actors[j] })
	indexes := make([]Index, 0, len(actors))
	for _, actor := range actors {
		indexes = append(indexes, Index{Actor: actor, Data: b.index[actor].ToBytes()})
	}
	batch := &Batch{Day: b.day, Raw: b.data, Data: Binary{Compressed: compressed, RawSize: int64(len(b.data))}, Indexes: indexes, Entries: uint32(b.count), Time: b.time}
	b.data = make([]byte, 0, 8<<20)
	b.index = make(map[uint32]*roaring.Bitmap)
	b.count = 0
	b.day = time.Time{}
	b.time = [2]uint32{}
	return batch, nil
}

func (b *Buffer) Snapshot() (time.Time, []byte, uint32) {
	return b.day, append([]byte(nil), b.data...), uint32(b.count)
}
