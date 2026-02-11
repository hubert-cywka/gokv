package record

import (
	"bytes"
	"encoding/binary"
	"io"
)

type Decoder struct {
	reader    io.Reader
	headerBuf header
}

func NewDecoder(reader io.Reader) *Decoder {
	return &Decoder{reader: reader}
}

func (d *Decoder) Decode(r *Record) error {
	if _, err := io.ReadFull(d.reader, d.headerBuf[:]); err != nil {
		return err
	}

	r.Kind = d.headerBuf[kindOffset]
	r.TxID = binary.LittleEndian.Uint64(d.headerBuf[txIDOffset : txIDOffset+txIDSize])
	keyLength := binary.LittleEndian.Uint16(d.headerBuf[keyLengthOffset : keyLengthOffset+keyLengthSize])
	valueLength := binary.LittleEndian.Uint32(d.headerBuf[valueLengthOffset : valueLengthOffset+valueLengthSize])

	expectedChecksum := make([]byte, checksumSize)
	copy(expectedChecksum, d.headerBuf[checksumOffset:])

	r.Key = growSlice(r.Key, int(keyLength))
	if _, err := io.ReadFull(d.reader, r.Key); err != nil {
		return err
	}

	r.Value = growSlice(r.Value, int(valueLength))
	if _, err := io.ReadFull(d.reader, r.Value); err != nil {
		return err
	}

	if !bytes.Equal(expectedChecksum, r.Checksum()) {
		return ChecksumMismatchError
	}

	return nil
}

func growSlice(b []byte, n int) []byte {
	if cap(b) < n {
		return make([]byte, n)
	}

	return b[:n]
}
