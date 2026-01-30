package data

import (
	"bytes"
	"encoding/binary"
	"io"
)

type Decoder struct {
	reader    io.Reader
	headerBuf RecordHeader
}

func NewDecoder(reader io.Reader) *Decoder {
	return &Decoder{reader: reader}
}

func (d *Decoder) Decode(r *Record) error {
	if _, err := io.ReadFull(d.reader, d.headerBuf[:]); err != nil {
		return err
	}

	offset := 0

	r.kind = d.headerBuf[offset]
	offset += kindSize

	keyLength := binary.LittleEndian.Uint16(d.headerBuf[offset : offset+keyLengthSize])
	offset += keyLengthSize

	valueLength := binary.LittleEndian.Uint32(d.headerBuf[offset : offset+valueLengthSize])
	offset += valueLengthSize

	expectedChecksum := make([]byte, checksumSize)
	copy(expectedChecksum, d.headerBuf[offset:offset+checksumSize])
	offset += checksumSize

	r.key = growSlice(r.Key(), int(keyLength))
	if _, err := io.ReadFull(d.reader, r.Key()); err != nil {
		return err
	}

	r.value = growSlice(r.Value(), int(valueLength))
	if _, err := io.ReadFull(d.reader, r.Value()); err != nil {
		return err
	}

	if !bytes.Equal(expectedChecksum, r.Checksum()) {
		return ErrChecksumMismatch
	}

	return nil
}

func growSlice(b []byte, n int) []byte {
	if cap(b) < n {
		return make([]byte, n)
	}

	return b[:n]
}
