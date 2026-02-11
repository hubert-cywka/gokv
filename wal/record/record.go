package record

import (
	"encoding/binary"
	"hash/crc32"
)

const (
	Tombstone uint8 = iota
	Value
)

const (
	kindSize   = 1
	kindOffset = 0

	txIDSize   = 8
	txIDOffset = kindOffset + kindSize

	keyLengthSize   = 2
	keyLengthOffset = txIDOffset + txIDSize

	valueLengthSize   = 4
	valueLengthOffset = keyLengthOffset + keyLengthSize

	checksumSize   = 4
	checksumOffset = valueLengthOffset + valueLengthSize

	headerSize = kindSize + txIDSize + keyLengthSize + valueLengthSize + checksumSize
)

type header [headerSize]byte

type Record struct {
	TxID  uint64
	Kind  uint8
	Key   []byte
	Value []byte
}

func NewValue(key string, value []byte, txID uint64) *Record {
	return newRecord(Value, key, value, txID)
}

func NewTombstone(key string, txID uint64) *Record {
	return newRecord(Tombstone, key, nil, txID)
}

func (r *Record) Checksum() []byte {
	h := crc32.NewIEEE()

	txIDBuffer := make([]byte, 8)
	binary.LittleEndian.PutUint64(txIDBuffer, uint64(r.TxID))

	_, _ = h.Write([]byte{r.Kind})
	_, _ = h.Write(txIDBuffer)
	_, _ = h.Write(r.Key)
	_, _ = h.Write(r.Value)

	return h.Sum(nil)
}

func newRecord(kind uint8, key string, value []byte, txID uint64) *Record {
	return &Record{
		Kind:  kind,
		Key:   []byte(key),
		Value: value,
		TxID:  txID,
	}
}
