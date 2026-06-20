package record

import (
	"bytes"
	"kv/test/assert"
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
				assert.NoError(t, err)

				decoded := &Record{}
				err = decoder.Decode(decoded)
				assert.NoError(t, err)

				assert.Equal(t, decoded.Kind, tt.original.Kind)
				assert.Equal(t, decoded.TxID, tt.original.TxID)
				assert.BytesEqual(t, decoded.Key, tt.original.Key)
				assert.BytesEqual(t, decoded.Value, tt.original.Value)
			})
		}
	})
}
