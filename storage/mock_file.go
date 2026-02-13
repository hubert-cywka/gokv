package storage

import (
	"errors"
	"io"
)

type MockFile struct {
	Data      []byte
	SyncCalls int

	offset int64
	closed bool
}

func NewMockFile() *MockFile {
	return &MockFile{
		Data: make([]byte, 0, 1024),
	}
}

func (m *MockFile) Write(p []byte) (n int, err error) {
	if m.closed {
		return 0, errors.New("file closed")
	}

	writeEnd := m.offset + int64(len(p))

	if writeEnd > int64(len(m.Data)) {
		if writeEnd > int64(cap(m.Data)) {
			newCap := int64(cap(m.Data)) * 2
			if newCap < writeEnd {
				newCap = writeEnd
			}
			newData := make([]byte, len(m.Data), newCap)
			copy(newData, m.Data)
			m.Data = newData
		}
		m.Data = m.Data[:writeEnd]
	}

	copy(m.Data[m.offset:], p)
	m.offset = writeEnd
	return len(p), nil
}

func (m *MockFile) Read(p []byte) (n int, err error) {
	if m.closed {
		return 0, errors.New("file closed")
	}

	if m.offset >= int64(len(m.Data)) {
		return 0, io.EOF
	}

	n = copy(p, m.Data[m.offset:])
	m.offset += int64(n)
	return n, nil
}

func (m *MockFile) Seek(offset int64, whence int) (int64, error) {
	if m.closed {
		return 0, errors.New("file closed")
	}

	var newOffset int64
	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekCurrent:
		newOffset = m.offset + offset
	case io.SeekEnd:
		newOffset = int64(len(m.Data)) + offset
	default:
		return 0, errors.New("invalid whence")
	}

	if newOffset < 0 {
		return 0, errors.New("negative position")
	}

	m.offset = newOffset
	return m.offset, nil
}

func (m *MockFile) Sync() error {
	m.SyncCalls++
	return nil
}

func (m *MockFile) Close() error {
	m.closed = true
	return nil
}
