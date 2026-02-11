package storage // TODO: Cleanup whole package

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"

	"github.com/rs/zerolog/log"
)

var ManifestChecksumMismatchError = errors.New("manifest: checksum mismatch")

type Manifest struct {
	path    string
	file    *os.File
	Content content
}

func NewManifest(path string) *Manifest {
	return &Manifest{
		path: path,
	}
}

func (m *Manifest) Open() error {
	file, err := os.OpenFile(m.path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		log.Error().Str("path", m.path).Err(err).Msg("manifest: failed to open")
		return err
	}

	m.file = file

	buf := make([]byte, manifestSize)
	_, err = io.ReadFull(m.file, buf)

	if err != io.EOF && !errors.Is(err, io.ErrUnexpectedEOF) {
		return err
	}

	val := binary.LittleEndian.Uint64(buf[logStartOffset : logStartOffset+logStartSize])

	m.Content = content{
		logStart: val,
	}

	expectedChecksum := buf[checksumOffset : checksumOffset+checksumSize]

	if !bytes.Equal(m.Content.Checksum(), expectedChecksum) {
		return ManifestChecksumMismatchError
	}

	return nil
}

func (m *Manifest) UpdateLogStart(start uint64) error {
	if m.file == nil {
		return nil
	}

	if _, err := m.file.Seek(logStartOffset, 0); err != nil {
		return err
	}

	buf := make([]byte, manifestSize)

	binary.LittleEndian.PutUint64(buf[logStartOffset:logStartOffset+logStartSize], start)
	copy(buf[checksumOffset:checksumOffset+checksumSize], m.Content.Checksum())

	if _, err := m.file.Write(buf); err != nil {
		return err
	}

	m.Content.logStart = start
	return m.file.Sync()
}

func (m *Manifest) Close() error {
	if m.file == nil {
		return nil
	}

	err := m.file.Close()
	m.file = nil
	return err
}

const (
	logStartOffset = 0
	logStartSize   = 8
	checksumOffset = logStartOffset + logStartSize
	checksumSize   = 4
	manifestSize   = logStartSize + checksumSize
)

type content struct {
	logStart uint64
}

func (mc *content) Checksum() []byte {
	h := crc32.NewIEEE()
	buffer := make([]byte, logStartSize)

	offset := 0
	binary.LittleEndian.PutUint64(buffer[offset:offset+logStartSize], mc.logStart)

	return h.Sum(buffer)
}
