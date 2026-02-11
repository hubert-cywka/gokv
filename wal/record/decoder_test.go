package record

import (
	"bytes"
	"encoding/binary"
	"io"
	"kv/test"
	"testing"
)

func TestDecoder_Decode(t *testing.T) {
	t.Run("it decodes a valid record correctly", func(t *testing.T) {
		want := NewValue("session_id", []byte("val_987654321"), 1)

		buf := new(bytes.Buffer)
		buf.WriteByte(want.Kind)

		txID := make([]byte, txIDSize)
		binary.LittleEndian.PutUint64(txID, uint64(want.TxID))
		buf.Write(txID)

		kLen := make([]byte, keyLengthSize)
		binary.LittleEndian.PutUint16(kLen, uint16(len(want.Key)))
		buf.Write(kLen)

		vLen := make([]byte, valueLengthSize)
		binary.LittleEndian.PutUint32(vLen, uint32(len(want.Value)))
		buf.Write(vLen)

		buf.Write(want.Checksum())
		buf.Write(want.Key)
		buf.Write(want.Value)

		decoder := NewDecoder(buf)
		got := &Record{}

		err := decoder.Decode(got)

		test.AssertNoError(t, err)
		test.AssertEqual(t, got.Kind, want.Kind)
		test.AssertBytesEqual(t, got.Key, want.Key)
		test.AssertBytesEqual(t, got.Value, want.Value)
	})

	t.Run("it returns error on checksum mismatch", func(t *testing.T) {
		want := NewValue("session_id", []byte("val_987654321"), 1)

		buf := new(bytes.Buffer)
		buf.WriteByte(want.Kind)

		txID := make([]byte, txIDSize)
		binary.LittleEndian.PutUint64(txID, uint64(want.TxID))
		buf.Write(txID)

		kLen := make([]byte, keyLengthSize)
		binary.LittleEndian.PutUint16(kLen, uint16(len(want.Key)))
		buf.Write(kLen)

		vLen := make([]byte, valueLengthSize)
		binary.LittleEndian.PutUint32(vLen, uint32(len(want.Value)))
		buf.Write(vLen)

		malformedChecksum := []byte("0000")
		buf.Write(malformedChecksum)
		buf.Write(want.Key)
		buf.Write(want.Value)

		decoder := NewDecoder(buf)
		got := &Record{}

		err := decoder.Decode(got)

		test.AssertError(t, err, ChecksumMismatchError)
	})

	t.Run("it returns EOF on empty reader", func(t *testing.T) {
		decoder := NewDecoder(new(bytes.Buffer))
		err := decoder.Decode(&Record{})
		test.AssertError(t, err, io.EOF)
	})

	t.Run("it returns error if header is truncated", func(t *testing.T) {
		incompleteHeader := []byte{1, 0, 5}
		decoder := NewDecoder(bytes.NewReader(incompleteHeader))

		err := decoder.Decode(&Record{})
		test.AssertError(t, err, io.ErrUnexpectedEOF)
	})
}
