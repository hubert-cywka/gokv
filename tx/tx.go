package tx

import (
	"kv/memstore/entry"
	"sync"
)

const (
	FROZEN_TX_ID = uint64(0)
	HALF_SPACE   = uint64(1 << 63)
)

type Transaction struct {
	ID uint64

	writes   []*entry.Entry
	manager  *TransactionManager
	snapshot Snapshot

	once sync.Once
}

func (tx *Transaction) Track(x *entry.Entry) {
	// TODO: Race condition
	tx.writes = append(tx.writes, x)
}

func (tx *Transaction) Commit() error {
	var err error

	tx.once.Do(func() {
		err = tx.manager.commit(tx.ID)
	})

	return err
}

func (tx *Transaction) Abort() {
	tx.once.Do(func() {
		for i := len(tx.writes) - 1; i >= 0; i-- {
			e := tx.writes[i]

			if e == nil {
				continue
			}

			if e.XMin() == tx.ID {
				e.SetXMax(tx.ID)
				continue
			}

			if e.XMax() == tx.ID {
				e.SetXMax(FROZEN_TX_ID)
				continue
			}
		}

		tx.manager.abort(tx.ID)
	})
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
