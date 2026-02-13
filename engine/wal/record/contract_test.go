package record

import (
	"bytes"
	"kv/test"
	"testing"
)

func TestEncodingContract(t *testing.T) {
	t.Run("it decodes previously encoded records", func(t *testing.T) {
		tests := []struct {
			name     string
			original *Record
		}{
			{"value", NewValue("Key", []byte("Value"), 1)},
			{"tombstone", NewTombstone("Key", 1)},
			{"commit", NewCommit(1)},
			{"freeze", NewFreeze("Key", 1)},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				buf := new(bytes.Buffer)
				encoder := NewEncoder(buf)
				decoder := NewDecoder(buf)

				err := encoder.Encode(tt.original)
				test.AssertNoError(t, err)

				decoded := &Record{}
				err = decoder.Decode(decoded)
				test.AssertNoError(t, err)

				test.AssertEqual(t, decoded.Kind, tt.original.Kind)
				test.AssertEqual(t, decoded.TxID, tt.original.TxID)
				test.AssertBytesEqual(t, decoded.Key, tt.original.Key)
				test.AssertBytesEqual(t, decoded.Value, tt.original.Value)
			})
		}
	})
}
