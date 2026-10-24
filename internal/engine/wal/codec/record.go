package codec

import (
	"hash/crc32"
	"kv/internal/engine/record"
	"kv/internal/engine/wal/codec/conversion"
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

func checksum(r *record.Record) uint32 {
	h := crc32.NewIEEE()

	_, _ = h.Write(conversion.Uint8ToBytes(r.Kind))
	_, _ = h.Write(conversion.Uint64ToBytes(r.TxID))
	_, _ = h.Write(r.Key)
	_, _ = h.Write(r.Value)

	return h.Sum32()
}
