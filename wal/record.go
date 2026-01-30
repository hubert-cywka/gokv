package wal

import (
	"encoding/binary"
	"io"
)

type RecordKind uint8

const (
	Delete RecordKind = iota
	Update
)

// TODO: Checksum

type Record struct {
	kind        RecordKind
	key         []byte
	value       []byte
	keyLength   uint16
	valueLength uint32
}

func NewRecord(key string, value []byte) *Record {
	return createRecord(Update, key, value)
}

func NewTombstone(key string) *Record {
	return createRecord(Delete, key, nil)
}

func (r *Record) Kind() RecordKind {
	return r.kind
}

func (r *Record) Value() []byte {
	return r.value
}

func (r *Record) Key() []byte {
	return r.key
}

func (r *Record) Write(writer io.Writer) error {
	key := r.Key()
	val := r.Value()
	totalSize := 1 + 2 + 4 + len(key) + len(val)

	buf := make([]byte, totalSize)
	off := 0

	size := 1
	buf[0] = uint8(r.Kind())
	off += size

	size = 2
	binary.LittleEndian.PutUint16(buf[off:off+size], r.keyLength)
	off += size

	size = 4
	binary.LittleEndian.PutUint32(buf[off:off+size], r.valueLength)
	off += size

	size = len(key)
	copy(buf[off:off+size], key)
	off += size

	size = len(val)
	copy(buf[off:off+size], val)
	off += size

	_, err := writer.Write(buf)
	return err
}

func (r *Record) Read(reader io.Reader) bool {
	err := binary.Read(reader, binary.LittleEndian, &r.kind)
	if err == io.EOF {
		return false
	}

	_ = binary.Read(reader, binary.LittleEndian, &r.keyLength)
	_ = binary.Read(reader, binary.LittleEndian, &r.valueLength)

	r.key = make([]byte, r.keyLength)
	_, _ = io.ReadFull(reader, r.key)

	r.value = make([]byte, r.valueLength)
	_, _ = io.ReadFull(reader, r.value)

	return true
}

func createRecord(kind RecordKind, key string, value []byte) *Record {
	return &Record{
		kind:        kind,
		key:         []byte(key),
		value:       value,
		keyLength:   uint16(len(key)),
		valueLength: uint32(len(value)),
	}
}
