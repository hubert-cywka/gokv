package tx

import (
	"sync"
)

const (
	HalfSpace = ID(1 << 63)
)

type Transaction struct {
	ID ID

	writes   []version
	manager  *Manager
	snapshot Snapshot

	once  sync.Once
	mutex sync.Mutex
}

func newTransaction(id ID, manager *Manager, snapshot Snapshot) *Transaction {
	return &Transaction{
		ID:       id,
		manager:  manager,
		snapshot: snapshot,
	}
}

func (tx *Transaction) Track(x version) {
	if x == nil {
		return
	}

	tx.mutex.Lock()
	defer tx.mutex.Unlock()

	tx.writes = append(tx.writes, x)
}

func (tx *Transaction) Commit() error {
	tx.mutex.Lock()
	defer tx.mutex.Unlock()

	var err error

	tx.once.Do(func() {
		err = tx.manager.commit(tx.ID)
	})

	return err
}

func (tx *Transaction) Abort() {
	tx.mutex.Lock()
	defer tx.mutex.Unlock()

	tx.once.Do(func() {
		for i := len(tx.writes) - 1; i >= 0; i-- {
			e := tx.writes[i]

			if e == nil {
				continue
			}

			if e.XMin() == tx.ID {
				e.TryKill(tx.ID)
				continue
			}

			if e.XMax() == tx.ID {
				e.Resurrect()
				continue
			}
		}

		tx.manager.abort(tx.ID)
	})
}

func (tx *Transaction) CanSee(xMin, xMax ID) bool {
	// Own insert
	if xMin == tx.ID && xMax.IsAlive() {
		return true
	}

	// Own delete
	if xMax == tx.ID {
		return false
	}

	if !xMin.IsFrozen() {
		// Inserted, but transaction is still activeTx
		if tx.snapshot.IsActive(xMin) {
			return false
		}

		// Inserted in the future
		if !xMin.Precedes(tx.snapshot.xMax) {
			return false
		}
	}

	// Old + never deleted
	if xMin.IsFrozen() && xMax.IsAlive() {
		return true
	}

	// Never deleted
	if xMax.IsAlive() {
		return true
	}

	// Deleted, but transaction still activeTx
	if tx.snapshot.IsActive(xMax) {
		return true
	}

	// Deleted in the future
	if !xMax.Precedes(tx.snapshot.xMax) {
		return true
	}

	// Deleted in the past
	return false
}
