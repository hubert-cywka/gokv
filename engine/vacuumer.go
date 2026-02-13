package engine

import (
	"kv/engine/mvcc"
	"kv/engine/tx"
	"kv/engine/wal"
	"kv/engine/wal/record"
	"sync"
)

const maxWorkers = 100

type Vacuumer struct {
	versionMap  *mvcc.VersionMap
	walAppender wal.Appender
}

func NewVacuumer(versionMap *mvcc.VersionMap, walAppender wal.Appender) *Vacuumer {
	return &Vacuumer{
		versionMap:  versionMap,
		walAppender: walAppender,
	}
}

func (v *Vacuumer) Vacuum(tm *tx.Manager) {
	horizon := tm.FindTxHorizon()

	var wg sync.WaitGroup
	semaphore := make(chan struct{}, maxWorkers)

	v.versionMap.Range(func(key string, ptr *mvcc.VersionChain) bool {
		semaphore <- struct{}{}

		wg.Go(func() {
			defer func() {
				<-semaphore
			}()

			head := ptr.Head()
			if head == nil {
				return
			}

			xMax := head.XMax()

			if v.canPrune(xMax, horizon) {
				v.versionMap.Remove(key)
				return
			}

			if xMin := head.XMin(); v.canFreeze(xMin, horizon) {
				v.freeze(head)
			}

			v.vacuumChain(head, horizon)
		})

		return true
	})

	wg.Wait()
}

func (v *Vacuumer) vacuumChain(head *mvcc.Version, horizon tx.ID) {
	curr := head
	for {
		next := curr.PreviousVersion()
		if next == nil {
			break
		}

		xMax := next.XMax()

		if v.canPrune(xMax, horizon) {
			curr.SetPreviousVersion(nil)
			break
		}

		if xMin := next.XMin(); v.canFreeze(xMin, horizon) {
			v.freeze(next)
		}

		curr = next
	}
}

func (v *Vacuumer) canFreeze(xMin tx.ID, horizon tx.ID) bool {
	return !xMin.IsFrozen() && xMin != horizon && xMin.Precedes(horizon)
}

func (v *Vacuumer) canPrune(xMax tx.ID, horizon tx.ID) bool {
	return !xMax.IsAlive() && xMax != horizon && xMax.Precedes(horizon)
}

func (v *Vacuumer) freeze(version *mvcc.Version) {
	freezeRecord := record.NewFreeze(version.Key, version.XMin().Uint64())

	if err := v.walAppender.Append(freezeRecord); err != nil {
		return
	}

	version.Freeze()
}
