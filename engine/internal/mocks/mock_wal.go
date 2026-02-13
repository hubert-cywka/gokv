package mocks

import (
	"kv/engine/wal/record"
)

type MockAppender struct {
	Records []*record.Record
	Err     error
}

func NewAppender() *MockAppender {
	return &MockAppender{}
}

func (m *MockAppender) Append(record *record.Record) error {
	if m.Err != nil {
		return m.Err
	}

	m.Records = append(m.Records, record)
	return nil
}
