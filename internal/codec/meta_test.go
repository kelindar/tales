package codec

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestManifestRoundTrip(t *testing.T) {
	manifest := &Manifest{Day: "2026-07-19", Writer: "0123456789abcdef", Chunks: []ChunkEntry{{
		Sequence: 0, Entries: 2, Time: [2]uint32{1, 2}, BitmapSize: 4,
		Data: Range{Offset: 4, Size: 5}, Size: 9, ETag: "etag",
		Actors: map[uint32]Range{7: {Offset: 0, Size: 4}},
	}}}
	data, err := Encode(manifest)
	require.NoError(t, err)
	require.Contains(t, string(data), `"sequence":"00000000000000000000"`)
	decoded, err := Decode[Manifest](data)
	require.NoError(t, err)
	require.NoError(t, ValidateManifest(decoded, manifest.Day, manifest.Writer))
	require.Equal(t, uint64(1), decoded.NextSequence())
	require.True(t, decoded.Chunks[0].Between(1, 2))
	require.False(t, decoded.Chunks[0].Between(3, 4))
}

func TestManifestValidation(t *testing.T) {
	validChunk := ChunkEntry{
		Sequence: 0, Entries: 1, Time: [2]uint32{0, 10}, BitmapSize: 4,
		Data: Range{Offset: 4, Size: 1}, Size: 5, ETag: "etag",
		Actors: map[uint32]Range{1: {Offset: 0, Size: 4}},
	}

	t.Run("identity", func(t *testing.T) {
		require.Error(t, ValidateManifest(nil, "d", "0123456789abcdef"))
		require.Error(t, ValidateManifest(&Manifest{Day: "x", Writer: "0123456789abcdef"}, "d", "0123456789abcdef"))
		require.Error(t, ValidateManifest(&Manifest{Day: "d", Writer: "bad"}, "d", "bad"))
	})

	t.Run("sequence", func(t *testing.T) {
		manifest := &Manifest{Day: "d", Writer: "0123456789abcdef", Chunks: []ChunkEntry{{Sequence: 1}}}
		require.Error(t, ValidateManifest(manifest, "d", "0123456789abcdef"))
	})

	t.Run("chunk errors", func(t *testing.T) {
		require.Error(t, ValidateChunk(ChunkEntry{}))
		require.Error(t, ValidateChunk(ChunkEntry{Entries: 1}))
		require.Error(t, ValidateChunk(ChunkEntry{Entries: 1, ETag: "e", Size: 1, Time: [2]uint32{2, 1}}))
		require.Error(t, ValidateChunk(ChunkEntry{Entries: 1, ETag: "e", Size: 1, Time: [2]uint32{0, MaxMillis + 1}}))
		require.Error(t, ValidateChunk(ChunkEntry{
			Entries: 1, ETag: "e", Size: 5, Time: [2]uint32{0, 1}, BitmapSize: 0,
			Data: Range{Offset: 0, Size: 5},
		}))
		require.Error(t, ValidateChunk(ChunkEntry{
			Entries: 1, ETag: "e", Size: 9, Time: [2]uint32{0, 1}, BitmapSize: 4,
			Data: Range{Offset: 4, Size: 4}, Actors: map[uint32]Range{1: {Offset: 0, Size: 4}},
		}))
		require.Error(t, ValidateChunk(ChunkEntry{
			Entries: 1, ETag: "e", Size: 5, Time: [2]uint32{0, 1}, BitmapSize: 4,
			Data: Range{Offset: 4, Size: 1}, Actors: nil,
		}))
		require.Error(t, ValidateChunk(ChunkEntry{
			Entries: 1, ETag: "e", Size: 5, Time: [2]uint32{0, 1}, BitmapSize: 4,
			Data: Range{Offset: 4, Size: 1}, Actors: map[uint32]Range{1: {Offset: 1, Size: 3}},
		}))
		require.NoError(t, ValidateChunk(validChunk))
	})

	t.Run("fixed width sequence", func(t *testing.T) {
		for _, input := range []string{
			`{"day":"d","writer":"w","chunks":[{"sequence":0}]}`,
			`{"day":"d","writer":"w","chunks":[{"sequence":"0"}]}`,
			`{"day":"d","writer":"w","chunks":[{"sequence":"0000000000000000000x"}]}`,
			`{"day":"d","writer":"w","chunks":[{"sequence":"99999999999999999999"}]}`,
		} {
			_, err := Decode[Manifest]([]byte(input))
			require.Error(t, err)
		}
	})

	t.Run("writer id casing", func(t *testing.T) {
		manifest := &Manifest{Day: "d", Writer: "ABCDEF0123456789"}
		require.Error(t, ValidateManifest(manifest, "d", manifest.Writer))
	})

	t.Run("next sequence empty", func(t *testing.T) {
		require.Equal(t, uint64(0), (&Manifest{}).NextSequence())
	})

	t.Run("compact metadata", func(t *testing.T) {
		require.Error(t, ValidateCompact(nil, "d"))
		require.Error(t, ValidateCompact(&CompactMetadata{}, "d"))

		meta := &CompactMetadata{
			Day: "d", PublishedAt: 1,
			Index:  ObjectRange{Key: "index", ETag: "etag", Size: 4},
			Actors: map[uint32]Range{1: {Offset: 0, Size: 4}},
			Sources: []CompactSource{{
				Writer: "0000000000000000", Base: 0, Sequence: 0, Entries: 1,
				Time: [2]uint32{0, 1}, Payload: ObjectRange{Key: "a", ETag: "etag", Size: 1}, Source: "a",
			}},
		}
		require.NoError(t, ValidateCompact(meta, "d"))

		meta.Sources[0].Sequence = 1
		require.Error(t, ValidateCompact(meta, "d"))

		meta.Sources = []CompactSource{
			{Writer: "0000000000000000", Base: 0, Sequence: 0, Entries: 1, Time: [2]uint32{0, 1}, Payload: ObjectRange{Key: "a", ETag: "etag", Size: 1}, Source: "a"},
			{Writer: "0000000000000000", Base: 1, Sequence: 3, Entries: 1, Time: [2]uint32{0, 1}, Payload: ObjectRange{Key: "b", ETag: "etag", Size: 1}, Source: "b"},
		}
		require.Error(t, ValidateCompact(meta, "d"))

		meta.Sources = []CompactSource{{
			Writer: "0000000000000000", Base: 0, Sequence: 0, Entries: 1, Time: [2]uint32{0, 1},
			Payload: ObjectRange{Key: "copied", ETag: "etag", Size: 1}, Source: "a", Copied: false,
		}}
		require.Error(t, ValidateCompact(meta, "d"))
	})

	t.Run("ordinal overflow", func(t *testing.T) {
		meta := &CompactMetadata{
			Day: "d", PublishedAt: 1,
			Index:  ObjectRange{Key: "index", ETag: "etag", Size: 1},
			Actors: map[uint32]Range{1: {Size: 1}},
			Sources: []CompactSource{
				{Writer: "0000000000000000", Base: 0, Sequence: 0, Entries: ^uint32(0), Payload: ObjectRange{Key: "a", ETag: "etag", Size: 1}, Source: "a"},
				{Writer: "0000000000000000", Base: uint64(^uint32(0)), Sequence: 1, Entries: 2, Payload: ObjectRange{Key: "b", ETag: "etag", Size: 1}, Source: "b"},
			},
		}
		require.Error(t, ValidateCompact(meta, "d"))
	})

	t.Run("actor range overflow", func(t *testing.T) {
		require.Error(t, validateActorRanges(map[uint32]Range{
			1: {Offset: 0, Size: math.MaxInt64},
			2: {Offset: math.MaxInt64, Size: 1},
		}, math.MaxInt64))
	})
}
