package wal

import "kv/engine/wal/record"

type Replayer interface {
	Replay(apply func(record.Record)) error
}
