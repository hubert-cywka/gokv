package wal

import "kv/engine/wal/record"

type Appender interface {
	Append(record *record.Record) error
}
