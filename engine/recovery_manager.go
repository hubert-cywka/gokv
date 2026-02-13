package engine

import (
	"kv/engine/mvcc"
	"kv/engine/tx"
	"kv/engine/wal"
	"kv/engine/wal/record"
	"sync"
)

func NewRecoveryManager(versionMap *mvcc.VersionMap, replayer wal.Replayer) *RecoveryManager {
	return &RecoveryManager{
		versionMap: versionMap,
		replayer:   replayer,
	}
}

type RecoveryManager struct {
	versionMap *mvcc.VersionMap
	replayer   wal.Replayer
	committed  map[uint64]struct{}
	lock       sync.Mutex
}

func (rm *RecoveryManager) Run() error {
	rm.lock.Lock()
	defer rm.lock.Unlock()

	rm.committed = make(map[uint64]struct{})

	if err := rm.replayer.Replay(rm.loadCommittedTransactions); err != nil {
		return err
	}

	if err := rm.replayer.Replay(rm.applyCommittedRecords); err != nil {
		return err
	}

	return nil
}

func (rm *RecoveryManager) applyCommittedRecords(r record.Record) {
	_, ok := rm.committed[r.TxID]

	if !ok {
		return
	}

	if r.Kind == record.Tombstone {
		rm.versionMap.Remove(string(r.Key))
		return
	}

	if r.Kind == record.Value {
		rm.versionMap.Remove(string(r.Key))
		chain := rm.versionMap.GetOrCreateChain(string(r.Key))
		newVersion := mvcc.NewVersion(string(r.Key), r.Value, tx.ID(r.TxID))
		chain.CompareHeadAndSwap(chain.Head(), newVersion)
	}

	if r.Kind == record.Freeze {
		rm.versionMap.Remove(string(r.Key))
		chain, _ := rm.versionMap.GetChain(string(r.Key))
		chain.Head().Freeze()
	}
}

func (rm *RecoveryManager) loadCommittedTransactions(r record.Record) {
	if r.Kind != record.Commit {
		return
	}

	rm.committed[r.TxID] = struct{}{}
}
