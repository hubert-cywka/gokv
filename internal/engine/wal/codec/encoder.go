package codec

import (
	"encoding/binary"
	"io"
	"kv/internal/engine/record"
)

type Encoder struct {
	writer    io.Writer
	headerBuf header
}

func NewEncoder(writer io.Writer) *Encoder {
	return &Encoder{writer: writer}
}

func (e *Encoder) Encode(r *record.Record) error {
	e.headerBuf[kindOffset] = r.Kind
	binary.LittleEndian.PutUint64(e.headerBuf[txIDOffset:txIDOffset+txIDSize], r.TxID)
	binary.LittleEndian.PutUint16(e.headerBuf[keyLengthOffset:keyLengthOffset+keyLengthSize], uint16(len(r.Key)))
	binary.LittleEndian.PutUint32(e.headerBuf[valueLengthOffset:valueLengthOffset+valueLengthSize], uint32(len(r.Value)))
	binary.LittleEndian.PutUint32(e.headerBuf[checksumOffset:checksumOffset+checksumSize], checksum(r))

	if _, err := e.writer.Write(e.headerBuf[:]); err != nil {
		return err
	}

	if _, err := e.writer.Write(r.Key); err != nil {
		return err
	}

	if _, err := e.writer.Write(r.Value); err != nil {
		return err
	}

	return nil
}
