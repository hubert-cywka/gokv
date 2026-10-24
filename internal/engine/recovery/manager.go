package recovery

import (
	"kv/internal/engine/mvcc"
	"kv/internal/engine/record"
	"kv/internal/engine/tx"
	"sync"

	"github.com/rs/zerolog/log"
)

func NewManager(versionMap *mvcc.VersionMap, walReplayer Replayer) *Manager {
	return &Manager{
		versionMap:  versionMap,
		walReplayer: walReplayer,
	}
}

type Manager struct {
	versionMap  *mvcc.VersionMap
	walReplayer Replayer
	committed   map[uint64]struct{}
	lock        sync.Mutex
}

func (rm *Manager) Recover() error {
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

func (rm *Manager) applyCommittedRecords(r record.Record) {
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

func (rm *Manager) applyValueRecord(key string, r record.Record) {
	chain := rm.versionMap.GetOrCreateChain(key)
	newVersion := mvcc.NewVersion(key, r.Value, tx.ID(r.TxID))
	chain.CompareHeadAndSwap(chain.Head(), newVersion)
}

func (rm *Manager) applyFreezeRecord(key string) {
	chain, _ := rm.versionMap.GetChain(key)

	if head := chain.Head(); head != nil {
		head.Freeze()
	}
}

func (rm *Manager) loadCommittedTransactions(r record.Record) {
	if r.Kind != record.Commit {
		return
	}

	rm.committed[r.TxID] = struct{}{}
}
