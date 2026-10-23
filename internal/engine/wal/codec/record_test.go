package codec

import (
	"kv/internal/engine/record"
	"kv/internal/test/assert"
	"testing"
)

func TestRecord_Checksum(t *testing.T) {
	t.Run("it changes checksum when key changes", func(t *testing.T) {
		r := record.NewValue("Key", []byte("Value"), 1)
		previous := checksum(r)

		r.Key = []byte("modified_key")
		current := checksum(r)

		assert.NotEqual(t, previous, current)
	})

	t.Run("it changes checksum when value changes", func(t *testing.T) {
		r := record.NewValue("Key", []byte("Value"), 1)
		previous := checksum(r)

		r.Value = []byte("modified_value")
		current := checksum(r)

		assert.NotEqual(t, previous, current)
	})

	t.Run("it changes checksum when kind changes", func(t *testing.T) {
		r := record.NewValue("Key", []byte("Value"), 1)
		previous := checksum(r)

		r.Kind = record.Tombstone
		current := checksum(r)

		assert.NotEqual(t, previous, current)
	})

	t.Run("it changes checksum when transaction ID changes", func(t *testing.T) {
		r := record.NewValue("Key", []byte("Value"), 1)
		previous := checksum(r)

		r.TxID = 9999999
		current := checksum(r)

		assert.NotEqual(t, previous, current)
	})
}
