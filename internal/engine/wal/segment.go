package wal

import (
	"os"
)

type Segment struct {
	path string
	file *os.File

	size     int64
	capacity int64
}

func NewSegment(path string, capacity int64) *Segment {
	return &Segment{
		capacity: capacity,
		path:     path,
		size:     -1,
	}
}

func (s *Segment) init() (*os.File, error) {
	file, err := os.OpenFile(s.path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)

	if err != nil {
		return nil, err
	}

	fi, err := file.Stat()

	if err != nil {
		defer func(file *os.File) {
			if err := file.Close(); err != nil {
				// TODO: Proper clean up
			}
		}(file)

		return nil, err
	}

	s.size = fi.Size()
	s.file = file

	return s.file, nil
}

func (s *Segment) File() (*os.File, error) {
	if s.file != nil {
		return s.file, nil
	}

	return s.init()
}

func (s *Segment) Size() (int64, error) {
	if s.size == -1 {
		if _, err := s.init(); err != nil {
			return 0, err
		}
	}

	return s.size, nil
}

func (s *Segment) Space() (int64, error) {
	size, err := s.Size()

	if err != nil {
		return 0, err
	}

	return s.capacity - size, nil
}

func (s *Segment) Read(buffer []byte) (n int, err error) {
	file, err := s.File()

	if err != nil {
		return 0, err
	}

	return file.Read(buffer)
}

func (s *Segment) Write(buffer []byte) (n int, err error) {
	var space int64

	if space, err = s.Space(); err != nil {
		return 0, err
	}

	if space <= 0 {
		return 0, nil
	}

	file, err := s.File()
	if err != nil {
		return 0, err
	}

	if int64(len(buffer)) > space {
		buffer = buffer[:space]
	}

	n, err = file.Write(buffer)
	s.size += int64(n)

	return n, err
}

func (s *Segment) Sync() error {
	file, err := s.File()

	if err != nil {
		return err
	}

	return file.Sync()
}

func (s *Segment) Close() error {
	file, err := s.File()

	if err != nil {
		return err
	}

	if err = file.Sync(); err != nil {
		return err
	}

	if err = file.Close(); err == nil {
		s.file = nil
	}

	return err
}

func (s *Segment) Seek(offset int64, whence int) (int64, error) {
	file, err := s.File()

	if err != nil {
		return 0, err
	}

	return file.Seek(offset, whence)
}
