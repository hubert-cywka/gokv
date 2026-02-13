package tx

import (
	"kv/engine/wal"
	"kv/engine/wal/record"
	"sync"
	"sync/atomic"
)

// TODO: Timeouts
// TODO: Add a way to reuse transactions (return a pointer to a transaction that is activeTx)

type ManagerOptions struct {
	ReservedIDsPerBatch   uint64
	MaxActiveTransactions uint16
}

type Manager struct {
	walAppender wal.Appender
	manifest    *Manifest

	activeTxCount atomic.Int32
	activeTx      sync.Map

	nextIDLock    sync.Mutex
	nextTxID      ID
	maxReservedID ID

	options ManagerOptions
}

func NewManager(manifest *Manifest, walAppender wal.Appender, options ManagerOptions) *Manager {
	return &Manager{
		manifest:    manifest,
		walAppender: walAppender,
		options:     options,
	}
}

func (tm *Manager) Begin() (*Transaction, error) {
	tm.nextIDLock.Lock()
	defer tm.nextIDLock.Unlock()

	txID, err := tm.allocateNextID()

	if err != nil {
		return nil, err
	}

	oldestTxID, found := tm.oldestActiveTx()
	tm.trackActive(txID)

	if !found {
		oldestTxID = txID
	}

	activeTx := tm.copyActiveTx()
	snapshot := newSnapshot(oldestTxID, txID, activeTx)
	return newTransaction(txID, tm, snapshot), nil
}

func (tm *Manager) FindTxHorizon() ID {
	tm.nextIDLock.Lock()
	defer tm.nextIDLock.Unlock()

	oldestTxID, found := tm.oldestActiveTx()

	if found {
		return oldestTxID
	}

	return tm.nextTxID + 1
}

func (tm *Manager) allocateNextID() (ID, error) {
	if !tm.isTxSlotAvailable() {
		return 0, MaxActiveTransactionsExceededError
	}

	if tm.maxReservedID <= tm.nextTxID {
		from, until, err := tm.manifest.ReserveIDs(tm.options.ReservedIDsPerBatch)

		if err != nil {
			return 0, err
		}

		tm.nextTxID = ID(from)
		tm.maxReservedID = ID(until)
	}

	for {
		tm.nextTxID++

		if tm.nextTxID.IsReserved() {
			continue
		}

		return tm.nextTxID, nil
	}
}

func (tm *Manager) commit(txID ID) error {
	if !tm.isActive(txID) {
		return TransactionNotActiveError
	}

	rec := record.NewCommit(txID.Uint64())
	if err := tm.walAppender.Append(rec); err != nil {
		return err
	}

	tm.stopTrackingActive(txID)
	return nil
}

func (tm *Manager) abort(txID ID) {
	tm.stopTrackingActive(txID)
}

func (tm *Manager) oldestActiveTx() (oldestTxID ID, found bool) {
	oldestTxID = IdFrozen
	found = false

	tm.activeTx.Range(func(key, value any) bool {
		id := key.(ID)
		if !found || id.Precedes(oldestTxID) {
			oldestTxID = id
			found = true
		}
		return true
	})

	return
}

func (tm *Manager) copyActiveTx() map[ID]struct{} {
	m := make(map[ID]struct{})

	tm.activeTx.Range(func(key, value any) bool {
		m[key.(ID)] = struct{}{}
		return true
	})

	return m
}

func (tm *Manager) trackActive(txID ID) {
	tm.activeTx.Store(txID, struct{}{})
	tm.activeTxCount.Add(1)
}

func (tm *Manager) stopTrackingActive(txID ID) {
	tm.activeTx.Delete(txID)
	tm.activeTxCount.Add(-1)
}

func (tm *Manager) isTxSlotAvailable() bool {
	return uint16(tm.activeTxCount.Load()) < tm.options.MaxActiveTransactions
}

func (tm *Manager) isActive(txID ID) bool {
	_, ok := tm.activeTx.Load(txID)
	return ok
}
