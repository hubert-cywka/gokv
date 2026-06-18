package statefile

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"sync"
)

var StatefileChecksumMismatchError = errors.New("statefile: checksum mismatch")

type Statefile[T any] struct {
	stream ioStream
	codec  Codec[T]
	mutex  sync.Mutex
}

func New[T any](stream ioStream, codec Codec[T]) *Statefile[T] {
	return &Statefile[T]{
		stream: stream,
		codec:  codec,
	}
}

func (s *Statefile[T]) Read() (T, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.read()
}

func (s *Statefile[T]) Write(v T) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.write(v)
}

func (s *Statefile[T]) read() (T, error) {
	if _, err := s.stream.Seek(0, io.SeekStart); err != nil {
		var zero T
		return zero, err
	}

	payloadSize := s.codec.Size()
	buf := make([]byte, payloadSize+4)

	n, err := io.ReadFull(s.stream, buf)

	if errors.Is(err, io.EOF) && n == 0 {
		return s.codec.Zero(), nil
	}

	if err != nil {
		var zero T
		return zero, err
	}

	payload := buf[:payloadSize]
	expected := binary.LittleEndian.Uint32(buf[payloadSize:])

	if crc32.ChecksumIEEE(payload) != expected {
		var zero T
		return zero, StatefileChecksumMismatchError
	}

	return s.codec.Unmarshal(payload), nil
}

func (s *Statefile[T]) write(v T) error {
	payloadSize := s.codec.Size()

	buf := make([]byte, payloadSize+4)

	payload := buf[:payloadSize]
	s.codec.Marshal(v, payload)

	binary.LittleEndian.PutUint32(
		buf[payloadSize:],
		crc32.ChecksumIEEE(payload),
	)

	if _, err := s.stream.Seek(0, io.SeekStart); err != nil {
		return err
	}

	if _, err := s.stream.Write(buf); err != nil {
		return err
	}

	return s.stream.Sync()
}
