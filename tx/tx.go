package tx

import (
	"sync"
	"sync/atomic"
)

const (
	FROZEN_TX_ID        = uint64(0)
	HALF_SPACE   uint64 = 1 << 63
)

type Transaction struct {
	ID uint64

	manager  *TransactionManager
	snapshot Snapshot

	committed atomic.Bool
	aborted   atomic.Bool
	once      sync.Once
}

func (tx *Transaction) Commit() error {
	var err error

	tx.once.Do(func() {
		err = tx.manager.commit(tx.ID)

		if err == nil {
			tx.committed.Store(true)
		}
	})

	return err
}

func (tx *Transaction) Abort() {
	tx.once.Do(func() {
		tx.manager.abort(tx.ID)
		tx.aborted.Store(true)
	})
}

func (tx *Transaction) Committed() bool {
	return tx.committed.Load()
}

func (tx *Transaction) CanSee(xMin, xMax uint64) bool {
	// Own insert
	if xMin == tx.ID && xMax == FROZEN_TX_ID {
		return true
	}

	// Own delete
	if xMax == tx.ID {
		return false
	}

	if xMin != FROZEN_TX_ID {
		// Inserted, but transaction is still active
		if tx.snapshot.IsActive(xMin) {
			return false
		}

		// Inserted in the future
		if !Precedes(xMin, tx.snapshot.xMax) {
			return false
		}
	}

	// Old + never deleted
	if xMin == FROZEN_TX_ID && xMax == FROZEN_TX_ID {
		return true
	}

	// Never deleted
	if xMax == FROZEN_TX_ID {
		return true
	}

	// Deleted, but transaction still active
	if tx.snapshot.IsActive(xMax) {
		return true
	}

	// Deleted in the future
	if !Precedes(xMax, tx.snapshot.xMax) {
		return true
	}

	// Deleted in the past
	return false
}

func Precedes(a, b uint64) bool {
	return b-a < HALF_SPACE
}
