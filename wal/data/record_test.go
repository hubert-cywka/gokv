package data

import (
	"kv/test"
	"testing"
)

func TestRecord_Checksum(t *testing.T) {
	t.Run("it changes checksum when key changes", func(t *testing.T) {
		record := NewValueRecord("key", []byte("value"))
		previous := record.Checksum()

		record.key = []byte("modified_key")
		current := record.Checksum()

		test.AssertBytesNotEqual(t, previous, current)
	})

	t.Run("it changes checksum when value changes", func(t *testing.T) {
		record := NewValueRecord("key", []byte("value"))
		previous := record.Checksum()

		record.value = []byte("modified_value")
		current := record.Checksum()

		test.AssertBytesNotEqual(t, previous, current)
	})

	t.Run("it changes checksum when kind changes", func(t *testing.T) {
		record := NewValueRecord("key", []byte("value"))
		previous := record.Checksum()

		record.kind = Delete
		current := record.Checksum()

		test.AssertBytesNotEqual(t, previous, current)
	})
}
