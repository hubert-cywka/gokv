package record

import (
	"kv/test"
	"testing"
)

func TestRecord_Checksum(t *testing.T) {
	t.Run("it changes checksum when key changes", func(t *testing.T) {
		record := NewValue("Key", []byte("Value"), 1)
		previous := record.Checksum()

		record.Key = []byte("modified_key")
		current := record.Checksum()

		test.AssertNotEqual(t, previous, current)
	})

	t.Run("it changes checksum when value changes", func(t *testing.T) {
		record := NewValue("Key", []byte("Value"), 1)
		previous := record.Checksum()

		record.Value = []byte("modified_value")
		current := record.Checksum()

		test.AssertNotEqual(t, previous, current)
	})

	t.Run("it changes checksum when kind changes", func(t *testing.T) {
		record := NewValue("Key", []byte("Value"), 1)
		previous := record.Checksum()

		record.Kind = Tombstone
		current := record.Checksum()

		test.AssertNotEqual(t, previous, current)
	})

	t.Run("it changes checksum when transaction ID changes", func(t *testing.T) {
		record := NewValue("Key", []byte("Value"), 1)
		previous := record.Checksum()

		record.TxID = 9999999
		current := record.Checksum()

		test.AssertNotEqual(t, previous, current)
	})
}
