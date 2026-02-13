package record

import (
	"bytes"
	"io"
	"kv/conversion"
	"kv/test"
	"testing"
)

func TestDecoder_Decode(t *testing.T) {
	writeRecord := func(r *Record, checksum uint32) *bytes.Buffer {
		buf := new(bytes.Buffer)

		buf.Write(conversion.Uint8ToBytes(r.Kind))
		buf.Write(conversion.Uint64ToBytes(r.TxID))
		buf.Write(conversion.Uint16ToBytes(uint16(len(r.Key))))
		buf.Write(conversion.Uint32ToBytes(uint32(len(r.Value))))
		buf.Write(conversion.Uint32ToBytes(checksum))

		buf.Write(r.Key)
		buf.Write(r.Value)
		return buf
	}

	t.Run("it decodes a valid record correctly", func(t *testing.T) {
		want := NewValue("session_id", []byte("val_987654321"), 1)
		buf := writeRecord(want, want.Checksum())

		got := &Record{}
		err := NewDecoder(buf).Decode(got)

		test.AssertNoError(t, err)
		test.AssertEqual(t, got.Kind, want.Kind)
		test.AssertBytesEqual(t, got.Key, want.Key)
		test.AssertBytesEqual(t, got.Value, want.Value)
	})

	t.Run("it returns error on checksum mismatch", func(t *testing.T) {
		want := NewValue("session_id", []byte("val_987654321"), 1)
		buf := writeRecord(want, want.Checksum()+1)

		err := NewDecoder(buf).Decode(&Record{})
		test.AssertError(t, err, ChecksumMismatchError)
	})

	t.Run("it returns EOF on empty reader", func(t *testing.T) {
		err := NewDecoder(new(bytes.Buffer)).Decode(&Record{})
		test.AssertError(t, err, io.EOF)
	})

	t.Run("it returns error if header is truncated", func(t *testing.T) {
		buf := bytes.NewReader([]byte{1, 0, 5})
		err := NewDecoder(buf).Decode(&Record{})
		test.AssertError(t, err, io.ErrUnexpectedEOF)
	})
}
