package wal

import (
	"encoding/binary"
	"kv/statefile"
)

type Manifest struct {
	file  *statefile.Statefile[state]
	state *state
}

func NewManifest(stream ioStream) *Manifest {
	return &Manifest{
		file: statefile.New(stream, stateCodec{}),
	}
}

func (m *Manifest) GetLogStart() (uint64, error) {
	if m.state != nil {
		return m.state.logStart, nil
	}

	s, err := m.file.Read()
	if err != nil {
		return 0, err
	}

	m.state = &s

	return s.logStart, nil
}

func (m *Manifest) UpdateLogStart(start uint64) error {
	s, err := m.file.Read()
	if err != nil {
		return err
	}

	s.logStart = start

	if err := m.file.Write(s); err != nil {
		return err
	}

	m.state = &s

	return nil
}

type state struct {
	logStart uint64
}

type stateCodec struct{}

func (stateCodec) Size() int {
	return 8
}

func (stateCodec) Zero() state {
	return state{}
}

func (stateCodec) Marshal(s state, buf []byte) {
	binary.LittleEndian.PutUint64(buf, s.logStart)
}

func (stateCodec) Unmarshal(buf []byte) state {
	return state{
		logStart: binary.LittleEndian.Uint64(buf),
	}
}
