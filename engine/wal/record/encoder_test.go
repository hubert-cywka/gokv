package record

import (
	"bytes"
	"encoding/binary"
	"io"
	"kv/test/assert"
	"testing"
)

func TestEncoder_Encode(t *testing.T) {
	verifyLayout := func(t *testing.T, data []byte, r *Record) {
		t.Helper()
		offset := 0

		assert.Equal(t, data[offset], r.Kind)
		offset += kindSize

		gotTxID := binary.LittleEndian.Uint64(data[offset : offset+txIDSize])
		assert.Equal(t, gotTxID, r.TxID)
		offset += txIDSize

		gotKeyLen := binary.LittleEndian.Uint16(data[offset : offset+keyLengthSize])
		assert.Equal(t, gotKeyLen, uint16(len(r.Key)))
		offset += keyLengthSize

		gotValLen := binary.LittleEndian.Uint32(data[offset : offset+valueLengthSize])
		assert.Equal(t, gotValLen, uint32(len(r.Value)))
		offset += valueLengthSize

		gotChecksum := binary.LittleEndian.Uint32(data[offset : offset+checksumSize])
		assert.Equal(t, gotChecksum, r.Checksum())
		offset += checksumSize

		assert.BytesEqual(t, data[offset:offset+len(r.Key)], r.Key)
		assert.BytesEqual(t, data[offset+len(r.Key):], r.Value)
	}

	t.Run("it encodes record with correct binary layout", func(t *testing.T) {
		buf := new(bytes.Buffer)
		record := NewValue("Key", []byte("Value"), 1)

		err := NewEncoder(buf).Encode(record)

		assert.NoError(t, err)
		verifyLayout(t, buf.Bytes(), record)
	})

	t.Run("it returns error on writer failure", func(t *testing.T) {
		encoder := NewEncoder(&limitedWriter{limit: 3})
		err := encoder.Encode(NewValue("long-Key", []byte("Value"), 1))

		assert.Error(t, err, io.ErrShortWrite)
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
