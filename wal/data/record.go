package data

import "hash/crc32"

const (
	Delete uint8 = iota
	Update
)

const (
	kindSize        = 1
	keyLengthSize   = 2
	valueLengthSize = 4
	checksumSize    = 4
	headerSize      = kindSize + keyLengthSize + valueLengthSize + checksumSize
)

type RecordHeader [headerSize]byte

type Record struct {
	kind  uint8
	key   []byte
	value []byte
}

func NewValueRecord(key string, value []byte) *Record {
	return createRecord(Update, key, value)
}

func NewTombstoneRecord(key string) *Record {
	return createRecord(Delete, key, nil)
}

func (r *Record) Kind() uint8 {
	return r.kind
}

func (r *Record) Value() []byte {
	return r.value
}

func (r *Record) Key() []byte {
	return r.key
}

func (r *Record) Checksum() []byte {
	h := crc32.NewIEEE()

	_, _ = h.Write([]byte{r.kind})
	_, _ = h.Write(r.key)
	_, _ = h.Write(r.value)

	return h.Sum(nil)
}

func createRecord(kind uint8, key string, value []byte) *Record {
	return &Record{
		kind:  kind,
		key:   []byte(key),
		value: value,
	}
}
