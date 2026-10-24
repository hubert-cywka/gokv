package recovery

import "kv/internal/engine/record"

type Replayer interface {
	Replay(apply func(record.Record)) error
}
