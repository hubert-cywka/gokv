package wal

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"kv/storage"
	"sync"
)

var ManifestChecksumMismatchError = errors.New("wal: manifest checksum mismatch")

const (
	logStartOffset = 0
	logStartSize   = 8
	checksumOffset = logStartOffset + logStartSize
	checksumSize   = 4
	manifestSize   = logStartSize + checksumSize
)

type Manifest struct {
	file  storage.File
	mutex sync.Mutex
	state *state
}

type state struct {
	logStart uint64
}

func NewManifest(file storage.File) *Manifest {
	return &Manifest{
		file: file,
	}
}

func (m *Manifest) UpdateLogStart(start uint64) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	s, err := m.read()
	if err != nil {
		return err
	}

	s.logStart = start

	if err = m.write(s); err != nil {
		return err
	}

	m.state = &s
	return nil
}

func (m *Manifest) GetLogStart() (uint64, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.state != nil {
		return m.state.logStart, nil
	}

	s, err := m.read()
	if err != nil {
		return 0, err
	}

	m.state = &s
	return s.logStart, nil
}

func (m *Manifest) read() (state, error) {
	if _, err := m.file.Seek(0, io.SeekStart); err != nil {
		return state{}, err
	}

	buf := make([]byte, manifestSize)

	n, err := io.ReadFull(m.file, buf)

	if errors.Is(err, io.EOF) && n == 0 {
		return state{logStart: 0}, nil
	}

	if err != nil {
		return state{}, err
	}

	logStart := binary.LittleEndian.Uint64(
		buf[logStartOffset : logStartOffset+logStartSize],
	)

	expectedChecksum := binary.LittleEndian.Uint32(
		buf[checksumOffset : checksumOffset+checksumSize],
	)

	s := state{
		logStart: logStart,
	}

	if s.checksum() != expectedChecksum {
		return state{}, ManifestChecksumMismatchError
	}

	return s, nil
}

func (m *Manifest) write(s state) error {
	buf := make([]byte, manifestSize)

	binary.LittleEndian.PutUint64(
		buf[logStartOffset:logStartOffset+logStartSize],
		s.logStart,
	)

	binary.LittleEndian.PutUint32(
		buf[checksumOffset:checksumOffset+checksumSize],
		s.checksum(),
	)

	if _, err := m.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	if _, err := m.file.Write(buf); err != nil {
		return err
	}

	return m.file.Sync()
}

func (s state) checksum() uint32 {
	var buf [manifestSize]byte
	binary.LittleEndian.PutUint64(buf[:], s.logStart)
	return crc32.ChecksumIEEE(buf[:])
}
