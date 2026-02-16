package engine

import (
	"kv/engine/mvcc"
	"kv/engine/tx"
	"kv/engine/wal"
	"kv/engine/wal/record"
	"sync"

	"github.com/rs/zerolog/log"
)

func NewRecoveryManager(versionMap *mvcc.VersionMap, walReplayer wal.Replayer) *RecoveryManager {
	return &RecoveryManager{
		versionMap:  versionMap,
		walReplayer: walReplayer,
	}
}

type RecoveryManager struct {
	versionMap  *mvcc.VersionMap
	walReplayer wal.Replayer
	committed   map[uint64]struct{}
	lock        sync.Mutex
}

func (rm *RecoveryManager) Run() error {
	rm.lock.Lock()
	defer rm.lock.Unlock()

	rm.committed = make(map[uint64]struct{})

	if err := rm.walReplayer.Replay(rm.loadCommittedTransactions); err != nil {
		return err
	}

	if err := rm.walReplayer.Replay(rm.applyCommittedRecords); err != nil {
		return err
	}

	return nil
}

func (rm *RecoveryManager) applyCommittedRecords(r record.Record) {
	if _, ok := rm.committed[r.TxID]; !ok {
		return
	}

	key := string(r.Key)

	switch r.Kind {
	case record.Tombstone:
		rm.versionMap.Remove(key)
	case record.Value:
		rm.applyValueRecord(key, r)
	case record.Freeze:
		rm.applyFreezeRecord(key)
	case record.Commit:
		// skip
	default:
		log.Error().Uint8("kind", r.Kind).Msg("recovery: unknown committed record kind")
	}
}

func (rm *RecoveryManager) applyValueRecord(key string, r record.Record) {
	chain := rm.versionMap.GetOrCreateChain(key)
	newVersion := mvcc.NewVersion(key, r.Value, tx.ID(r.TxID))
	chain.CompareHeadAndSwap(chain.Head(), newVersion)
}

func (rm *RecoveryManager) applyFreezeRecord(key string) {
	rm.versionMap.Remove(key)
	chain, _ := rm.versionMap.GetChain(key)

	if head := chain.Head(); head != nil {
		head.Freeze()
	}
}

func (rm *RecoveryManager) loadCommittedTransactions(r record.Record) {
	if r.Kind != record.Commit {
		return
	}

	rm.committed[r.TxID] = struct{}{}
}
