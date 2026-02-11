package record

import (
	"bytes"
	"kv/test"
	"testing"
)

func TestEncodingContract(t *testing.T) {
	t.Run("it decodes previously encoded record", func(t *testing.T) {
		buf := new(bytes.Buffer)
		encoder := NewEncoder(buf)
		decoder := NewDecoder(buf)

		original := NewValue("Key", []byte("Value"), 1)

		err := encoder.Encode(original)
		test.AssertNoError(t, err)

		decoded := &Record{}
		err = decoder.Decode(decoded)
		test.AssertNoError(t, err)

		test.AssertEqual(t, decoded.Kind, original.Kind)
		test.AssertEqual(t, decoded.TxID, original.TxID)
		test.AssertBytesEqual(t, decoded.Key, original.Key)
		test.AssertBytesEqual(t, decoded.Value, original.Value)
	})
}
