package record

import (
	"bytes"
	"encoding/binary"
	"io"
	"kv/test"
	"kv/tx"
	"testing"
)

func TestEncoder_Encode(t *testing.T) {
	t.Run("it encodes record with correct binary layout", func(t *testing.T) {
		buf := new(bytes.Buffer)
		encoder := NewEncoder(buf)

		txID := 1
		key := "Key"
		val := []byte("Value")
		record := NewValue(key, val, txID)

		err := encoder.Encode(record)
		test.AssertNoError(t, err)

		data := buf.Bytes()
		offset := 0

		expectedTotalLen := headerSize + len(key) + len(val)
		test.AssertEqual(t, len(data), expectedTotalLen)

		test.AssertEqual(t, data[offset], Value)
		offset += kindSize

		gotTxID := binary.LittleEndian.Uint64(data[offset : offset+txIDSize])
		offset += txIDSize
		test.AssertEqual(t, gotTxID, txID)

		gotKeyLen := binary.LittleEndian.Uint16(data[offset : offset+keyLengthSize])
		offset += keyLengthSize
		test.AssertEqual(t, gotKeyLen, uint16(len(key)))

		gotValLen := binary.LittleEndian.Uint32(data[offset : offset+valueLengthSize])
		offset += valueLengthSize
		test.AssertEqual(t, gotValLen, uint32(len(val)))

		test.AssertBytesEqual(t, data[offset:offset+checksumSize], record.Checksum())
		offset += checksumSize

		test.AssertBytesEqual(t, data[offset:offset+len(key)], []byte(key))
		test.AssertBytesEqual(t, data[offset+len(key):], val)
	})

	t.Run("it returns error on writer failure", func(t *testing.T) {
		errWriter := &limitedWriter{limit: 3}
		encoder := NewEncoder(errWriter)

		record := NewValue("long-Key", []byte("Value"), 1)
		err := encoder.Encode(record)

		if err == nil {
			t.Error("expected error when writer fails, but got nil")
		}
	})
}

type limitedWriter struct {
	limit int
}

func (w *limitedWriter) Write(p []byte) (n int, err error) {
	if len(p) > w.limit {
		return 0, io.ErrShortWrite
	}
	w.limit -= len(p)
	return len(p), nil
}
