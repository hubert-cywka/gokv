package tx

import (
	"encoding/binary"
	"kv/statefile"
)

type Manifest struct {
	file *statefile.Statefile[state]
}

func NewManifest(stream ioStream) *Manifest {
	return &Manifest{
		file: statefile.New(stream, stateCodec{}),
	}
}

func (m *Manifest) LastReservedID() (uint64, error) {
	s, err := m.file.Read()
	if err != nil {
		return 0, err
	}

	return s.reservedUntil, nil
}

func (m *Manifest) ReserveIDs(count uint64) (from, until uint64, err error) {
	s, err := m.file.Read()
	if err != nil {
		return 0, 0, err
	}

	from = s.reservedUntil + 1

	s.reservedUntil += count

	if err := m.file.Write(s); err != nil {
		return 0, 0, err
	}

	return from, s.reservedUntil, nil
}

type state struct {
	reservedUntil uint64
}

type stateCodec struct{}

func (stateCodec) Size() int {
	return 8
}

func (stateCodec) Zero() state {
	return state{}
}

func (stateCodec) Marshal(s state, buf []byte) {
	binary.LittleEndian.PutUint64(buf, s.reservedUntil)
}

func (stateCodec) Unmarshal(buf []byte) state {
	return state{
		reservedUntil: binary.LittleEndian.Uint64(buf),
	}
}
