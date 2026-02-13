package conversion

import (
	"bytes"
	"kv/test"
	"testing"
)

func TestUint64ToBytes(t *testing.T) {
	t.Run("it converts to little endian", func(t *testing.T) {
		val := uint64(0x0102030405060708)
		expected := []byte{0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01}

		got := Uint64ToBytes(val)

		test.AssertEqual(t, bytes.Equal(got, expected), true)
		test.AssertEqual(t, len(got), 8)
	})

	t.Run("it handles zero values", func(t *testing.T) {
		test.AssertEqual(t, bytes.Equal(Uint64ToBytes(0), make([]byte, 8)), true)
	})

	t.Run("it handles max values", func(t *testing.T) {
		max64 := ^uint64(0)
		test.AssertEqual(t, bytes.Equal(Uint64ToBytes(max64), bytes.Repeat([]byte{0xff}, 8)), true)
	})
}

func TestUint32ToBytes(t *testing.T) {
	t.Run("it converts to little endian", func(t *testing.T) {
		val := uint32(0x01020304)
		expected := []byte{0x04, 0x03, 0x02, 0x01}

		got := Uint32ToBytes(val)

		test.AssertEqual(t, bytes.Equal(got, expected), true)
		test.AssertEqual(t, len(got), 4)
	})

	t.Run("it handles zero values", func(t *testing.T) {
		test.AssertEqual(t, bytes.Equal(Uint32ToBytes(0), make([]byte, 4)), true)
	})

	t.Run("it handles max values", func(t *testing.T) {
		max32 := ^uint32(0)
		test.AssertEqual(t, bytes.Equal(Uint32ToBytes(max32), bytes.Repeat([]byte{0xff}, 4)), true)
	})
}

func TestUint16ToBytes(t *testing.T) {
	t.Run("it converts to little endian", func(t *testing.T) {
		val := uint16(0x0102)
		expected := []byte{0x02, 0x01}

		got := Uint16ToBytes(val)

		test.AssertEqual(t, bytes.Equal(got, expected), true)
		test.AssertEqual(t, len(got), 2)
	})

	t.Run("it handles zero values", func(t *testing.T) {
		test.AssertEqual(t, bytes.Equal(Uint16ToBytes(0), make([]byte, 2)), true)
	})

	t.Run("it handles max values", func(t *testing.T) {
		max16 := ^uint16(0)
		test.AssertEqual(t, bytes.Equal(Uint16ToBytes(max16), bytes.Repeat([]byte{0xff}, 2)), true)
	})
}

func TestUint8ToBytes(t *testing.T) {
	t.Run("it converts to single byte slice", func(t *testing.T) {
		val := uint8(0x42)
		expected := []byte{0x42}

		got := Uint8ToBytes(val)

		test.AssertEqual(t, bytes.Equal(got, expected), true)
		test.AssertEqual(t, len(got), 1)
	})

	t.Run("it handles zero values", func(t *testing.T) {
		test.AssertEqual(t, bytes.Equal(Uint8ToBytes(0), make([]byte, 1)), true)
	})

	t.Run("it handles max values", func(t *testing.T) {
		max8 := ^uint8(0)
		test.AssertEqual(t, bytes.Equal(Uint8ToBytes(max8), bytes.Repeat([]byte{0xff}, 1)), true)
	})
}
