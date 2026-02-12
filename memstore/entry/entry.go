package entry

import (
	"sync/atomic"
)

type Entry struct {
	Key   string
	Value []byte

	xMin *atomic.Uint64
	xMax *atomic.Uint64
	prev atomic.Pointer[Entry]
}

func New(key string, value []byte, txID uint64) Entry {
	xMin := &atomic.Uint64{}
	xMax := &atomic.Uint64{}

	xMin.Store(txID)
	xMax.Store(0) // TODO: tx.FROZEN_TX_ID)

	return Entry{
		Key:   key,
		Value: value,
		xMin:  xMin,
		xMax:  xMax,
	}
}

func (e *Entry) Prev() *Entry {
	return e.prev.Load()
}

func (e *Entry) SetPrev(p *Entry) {
	e.prev.Store(p)
}

func (e *Entry) XMin() uint64 {
	return e.xMin.Load()
}

func (e *Entry) SetXMin(x uint64) {
	e.xMin.Store(x)
}

func (e *Entry) CompareAndSetXMin(old uint64, new uint64) (set bool) {
	return e.xMin.CompareAndSwap(old, new)
}

func (e *Entry) XMax() uint64 {
	return e.xMax.Load()
}

func (e *Entry) SetXMax(x uint64) {
	e.xMax.Store(x)
}

func (e *Entry) CompareAndSetXMax(old uint64, new uint64) (set bool) {
	return e.xMax.CompareAndSwap(old, new)
}
