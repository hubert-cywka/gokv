package tx

import (
	"kv/assert"
	"kv/storage/mocks"
	"testing"
)

func TestManifest(t *testing.T) {
	t.Run("it initializes with zero if file is empty", func(t *testing.T) {
		file := mocks.NewFile()
		manifest := NewManifest(file)

		reserved, err := manifest.LastReservedID()
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), reserved)
	})

	t.Run("it reserves IDs and persists them", func(t *testing.T) {
		file := mocks.NewFile()
		oldManifest := NewManifest(file)

		_, until, err := oldManifest.ReserveIDs(100)
		assert.NoError(t, err)
		assert.Equal(t, uint64(100), until)

		newManifest := NewManifest(file)
		reserved, err := newManifest.LastReservedID()
		assert.NoError(t, err)
		assert.Equal(t, uint64(100), reserved)
	})

	t.Run("it increments existing reserved IDs", func(t *testing.T) {
		file := mocks.NewFile()
		manifest := NewManifest(file)

		_, _, _ = manifest.ReserveIDs(50)
		_, until, err := manifest.ReserveIDs(50)

		assert.NoError(t, err)
		assert.Equal(t, uint64(100), until)
	})

	t.Run("it does not waste IDs when fetching new range", func(t *testing.T) {
		file := mocks.NewFile()
		manifest := NewManifest(file)

		_, firstUntil, _ := manifest.ReserveIDs(50)
		secondFrom, _, _ := manifest.ReserveIDs(50)

		assert.Equal(t, firstUntil+1, secondFrom)
	})

	t.Run("it detects checksum corruption", func(t *testing.T) {
		file := mocks.NewFile()
		manifest := NewManifest(file)

		_, _, _ = manifest.ReserveIDs(100)
		file.Data[2] = ^file.Data[2]
		_, err := manifest.LastReservedID()

		assert.Error(t, err, ManifestChecksumMismatchError)
	})
}
