package wal

import "kv/internal/engine/wal/record"

type Replayer interface {
	Replay(apply func(record.Record)) error
}
