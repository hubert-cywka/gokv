package mvcc

import (
	"kv/engine/tx"
	"sync/atomic"
)

type Version struct {
	Key   string
	Value []byte

	xMin *atomic.Uint64
	xMax *atomic.Uint64
	prev atomic.Pointer[Version]
}

func NewVersion(key string, value []byte, txID tx.ID) *Version {
	xMin := &atomic.Uint64{}
	xMax := &atomic.Uint64{}

	xMin.Store(txID.Uint64())
	xMax.Store(tx.IdAlive.Uint64())

	return &Version{
		Key:   key,
		Value: value,
		xMin:  xMin,
		xMax:  xMax,
	}
}

func (v *Version) XMin() tx.ID {
	return tx.ID(v.xMin.Load())
}

func (v *Version) XMax() tx.ID {
	return tx.ID(v.xMax.Load())
}

func (v *Version) PreviousVersion() *Version {
	return v.prev.Load()
}

func (v *Version) SetPreviousVersion(p *Version) {
	v.prev.Store(p)
}

func (v *Version) Freeze() {
	v.xMin.Store(tx.IdFrozen.Uint64())
}

func (v *Version) Resurrect() {
	v.xMax.Store(tx.IdAlive.Uint64())
}

func (v *Version) TryKill(x tx.ID) (ok bool) {
	return v.xMax.CompareAndSwap(tx.IdAlive.Uint64(), x.Uint64())
}
