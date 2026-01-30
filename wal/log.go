package wal

type Log interface {
	Append(record *Record) error
	Read() ([]Record, error)
}
