package wal

import "kv/internal/engine/wal/record"

type Appender interface {
	Append(record *record.Record) error
}
