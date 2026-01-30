package data

import (
	"encoding/binary"
	"io"
)

type Encoder struct {
	writer    io.Writer
	headerBuf RecordHeader
}

func NewEncoder(writer io.Writer) *Encoder {
	return &Encoder{writer: writer}
}

func (e *Encoder) Encode(r *Record) error {
	offset := 0
	e.headerBuf[offset] = r.Kind()
	offset += kindSize

	binary.LittleEndian.PutUint16(e.headerBuf[offset:offset+keyLengthSize], uint16(len(r.Key())))
	offset += keyLengthSize

	binary.LittleEndian.PutUint32(e.headerBuf[offset:offset+valueLengthSize], uint32(len(r.Value())))
	offset += valueLengthSize

	copy(e.headerBuf[offset:], r.Checksum())

	if _, err := e.writer.Write(e.headerBuf[:]); err != nil {
		return err
	}

	if _, err := e.writer.Write(r.Key()); err != nil {
		return err
	}

	if _, err := e.writer.Write(r.Value()); err != nil {
		return err
	}

	return nil
}
