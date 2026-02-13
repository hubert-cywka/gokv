package record

import (
	"bytes"
	"encoding/binary"
	"io"
	"kv/test"
	"testing"
)

func TestEncoder_Encode(t *testing.T) {
	verifyLayout := func(t *testing.T, data []byte, r *Record) {
		t.Helper()
		offset := 0

		test.AssertEqual(t, data[offset], r.Kind)
		offset += kindSize

		gotTxID := binary.LittleEndian.Uint64(data[offset : offset+txIDSize])
		test.AssertEqual(t, gotTxID, r.TxID)
		offset += txIDSize

		gotKeyLen := binary.LittleEndian.Uint16(data[offset : offset+keyLengthSize])
		test.AssertEqual(t, gotKeyLen, uint16(len(r.Key)))
		offset += keyLengthSize

		gotValLen := binary.LittleEndian.Uint32(data[offset : offset+valueLengthSize])
		test.AssertEqual(t, gotValLen, uint32(len(r.Value)))
		offset += valueLengthSize

		gotChecksum := binary.LittleEndian.Uint32(data[offset : offset+checksumSize])
		test.AssertEqual(t, gotChecksum, r.Checksum())
		offset += checksumSize

		test.AssertBytesEqual(t, data[offset:offset+len(r.Key)], r.Key)
		test.AssertBytesEqual(t, data[offset+len(r.Key):], r.Value)
	}

	t.Run("it encodes record with correct binary layout", func(t *testing.T) {
		buf := new(bytes.Buffer)
		record := NewValue("Key", []byte("Value"), 1)

		err := NewEncoder(buf).Encode(record)

		test.AssertNoError(t, err)
		verifyLayout(t, buf.Bytes(), record)
	})

	t.Run("it returns error on writer failure", func(t *testing.T) {
		encoder := NewEncoder(&limitedWriter{limit: 3})
		err := encoder.Encode(NewValue("long-Key", []byte("Value"), 1))

		test.AssertError(t, err, io.ErrShortWrite)
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
