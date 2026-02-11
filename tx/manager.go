package tx

import (
	"errors"
	"sync"
)

// TODO: Timeouts
// TODO: Persistence
// TODO: Rollbacks on abort

var TransactionNotActiveError = errors.New("transaction not active")

type TransactionManager struct {
	mutex  sync.Mutex
	nextID uint64
	active map[uint64]struct{}
}

func NewTransactionManager() *TransactionManager {
	return &TransactionManager{
		active: make(map[uint64]struct{}),
	}
}

func (tm *TransactionManager) Begin() *Transaction {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	id := tm.allocateID()
	tm.active[id] = struct{}{}
	oldest, found := tm.oldestActive()

	if !found {
		oldest = id
	}

	snapshot := Snapshot{
		xMin:   oldest,
		xMax:   id,
		active: tm.copyActive(),
	}

	return &Transaction{
		ID:       id,
		snapshot: snapshot,
		manager:  tm,
	}
}

func (tm *TransactionManager) Horizon() uint64 {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	oldest, found := tm.oldestActive()

	if found {
		return oldest
	}

	return tm.nextID + 1
}

func (tm *TransactionManager) oldestActive() (oldestID uint64, found bool) {
	if len(tm.active) == 0 {
		return 0, false
	}

	oldestID = FROZEN_TX_ID
	found = true

	for id := range tm.active {
		if oldestID == FROZEN_TX_ID || Precedes(id, oldestID) {
			oldestID = id
		}
	}

	return
}

func (tm *TransactionManager) commit(txID uint64) error {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	if _, ok := tm.active[txID]; !ok {
		return TransactionNotActiveError
	}

	delete(tm.active, txID)
	return nil
}

func (tm *TransactionManager) abort(txID uint64) {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	if _, ok := tm.active[txID]; ok {
		delete(tm.active, txID)
	}
}

func (tm *TransactionManager) allocateID() uint64 {
	for {
		tm.nextID++

		if tm.nextID == FROZEN_TX_ID {
			continue
		}

		return tm.nextID
	}
}

func (tm *TransactionManager) copyActive() map[uint64]struct{} {
	m := make(map[uint64]struct{}, len(tm.active))

	for id := range tm.active {
		m[id] = struct{}{}
	}

	return m
}
