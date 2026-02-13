package tx

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"kv/storage"
	"sync"
)

const (
	reservedUntilOffset = 0
	reservedUntilSize   = 8
	checksumOffset      = reservedUntilOffset + reservedUntilSize
	checksumSize        = 4
	manifestSize        = reservedUntilSize + checksumSize
)

type Manifest struct {
	file  storage.File
	mutex sync.Mutex
}

type state struct {
	reservedFrom  uint64
	reservedUntil uint64
}

func NewManifest(file storage.File) *Manifest {
	return &Manifest{
		file: file,
	}
}

func (m *Manifest) LastReservedID() (uint64, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	c, err := m.read()

	if err != nil {
		return 0, err
	}

	return c.reservedUntil, nil
}

func (m *Manifest) ReserveIDs(count uint64) (from, until uint64, err error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	c, err := m.read()

	if err != nil {
		return 0, 0, err
	}

	c.reservedFrom = c.reservedUntil + 1
	c.reservedUntil += count

	if err = m.write(c); err != nil {
		return 0, 0, err
	}

	return c.reservedFrom, c.reservedUntil, nil
}

func (m *Manifest) read() (state, error) {
	if _, err := m.file.Seek(0, io.SeekStart); err != nil {
		return state{}, err
	}

	buf := make([]byte, manifestSize)

	n, err := io.ReadFull(m.file, buf)

	if errors.Is(err, io.EOF) && n == 0 {
		return state{reservedUntil: 0}, nil
	}

	if err != nil {
		return state{}, err
	}

	reservedUntil := binary.LittleEndian.Uint64(buf[reservedUntilOffset : reservedUntilOffset+reservedUntilSize])
	expectedChecksum := binary.LittleEndian.Uint32(buf[checksumOffset:])

	c := state{
		reservedUntil: reservedUntil,
	}

	if c.checksum() != expectedChecksum {
		return state{}, ManifestChecksumMismatchError
	}

	return c, nil
}

func (m *Manifest) write(s state) error {
	buf := make([]byte, manifestSize)

	binary.LittleEndian.PutUint64(buf[reservedUntilOffset:reservedUntilOffset+reservedUntilSize], s.reservedUntil)
	binary.LittleEndian.PutUint32(buf[checksumOffset:checksumOffset+checksumSize], s.checksum())

	if _, err := m.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	if _, err := m.file.Write(buf); err != nil {
		return err
	}

	if err := m.file.Sync(); err != nil {
		return err
	}

	return nil
}

func (s state) checksum() uint32 {
	var buf [manifestSize]byte
	binary.LittleEndian.PutUint64(buf[:], s.reservedUntil)
	return crc32.ChecksumIEEE(buf[:])
}
