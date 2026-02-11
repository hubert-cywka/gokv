package entry

import (
	"kv/tx"
	"sync/atomic"
)

type Entry struct {
	XMin *atomic.Uint64
	XMax *atomic.Uint64

	Key   string
	Value []byte

	Prev atomic.Pointer[Entry]
}

func New(key string, value []byte, txID uint64) Entry {
	xMin := &atomic.Uint64{}
	xMax := &atomic.Uint64{}

	xMin.Store(txID)
	xMax.Store(tx.FROZEN_TX_ID)

	return Entry{
		Key:   key,
		Value: value,
		XMin:  xMin,
		XMax:  xMax,
	}
}
