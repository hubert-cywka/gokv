package memstore

import (
	"kv/memstore/entry"
	"kv/tx"
	"sync"
	"sync/atomic"
)

// TODO: Handle same key updates in the same transaction

type MemStore struct {
	storage *sync.Map
}

func New(storage *sync.Map) *MemStore {
	return &MemStore{
		storage: storage,
	}
}

func (s *MemStore) Get(key string, t *tx.Transaction) ([]byte, error) {
	val, ok := s.storage.Load(key)

	if !ok {
		return nil, KeyNotFoundError
	}

	curr := val.(*atomic.Pointer[entry.Entry]).Load()

	for curr != nil {
		xMin := curr.XMin.Load()
		xMax := curr.XMax.Load()

		if t.CanSee(xMin, xMax) {
			if curr.Value == nil {
				return nil, KeyNotFoundError
			}

			return curr.Value, nil
		}

		curr = curr.Prev.Load()
	}

	return nil, KeyNotFoundError
}

func (s *MemStore) Set(key string, value []byte, t *tx.Transaction) error {
	actual, _ := s.storage.LoadOrStore(key, &atomic.Pointer[entry.Entry]{})
	ptr := actual.(*atomic.Pointer[entry.Entry])

	for {
		latest := ptr.Load()

		if latest != nil {
			if latest.XMax.Load() != tx.FROZEN_TX_ID {
				return SerializationError
			}

			if !t.CanSee(latest.XMin.Load(), tx.FROZEN_TX_ID) {
				return SerializationError
			}

			if !latest.XMax.CompareAndSwap(tx.FROZEN_TX_ID, t.ID) {
				return SerializationError
			}
		}

		newEntry := entry.New(key, value, t.ID)
		newEntry.Prev.Store(latest)

		if ptr.CompareAndSwap(latest, &newEntry) {
			return nil
		}

		if latest != nil {
			latest.XMax.Store(tx.FROZEN_TX_ID)
		}
	}
}

func (s *MemStore) Delete(key string, t *tx.Transaction) error {
	val, ok := s.storage.Load(key)
	if !ok {
		return KeyNotFoundError
	}

	ptr := val.(*atomic.Pointer[entry.Entry])

	for {
		latest := ptr.Load()

		if latest == nil {
			return KeyNotFoundError
		}

		if latest.XMax.Load() != tx.FROZEN_TX_ID {
			return SerializationError
		}

		if !t.CanSee(latest.XMin.Load(), tx.FROZEN_TX_ID) {
			return SerializationError
		}

		if !latest.XMax.CompareAndSwap(tx.FROZEN_TX_ID, t.ID) {
			return SerializationError
		}

		return nil
	}
}

func (s *MemStore) Vacuum(tm *tx.TransactionManager) {
	horizon := tm.Horizon()

	s.storage.Range(func(key, val interface{}) bool {
		ptr := val.(*atomic.Pointer[entry.Entry])
		head := ptr.Load()

		if head == nil {
			return true
		}

		xMax := head.XMax.Load()
		if xMax != tx.FROZEN_TX_ID && xMax != horizon && tx.Precedes(xMax, horizon) {
			ptr.CompareAndSwap(head, nil)
			return true
		}

		xMin := head.XMin.Load()
		if xMin != tx.FROZEN_TX_ID && xMin != horizon && tx.Precedes(xMin, horizon) {
			head.XMin.Store(tx.FROZEN_TX_ID)
		}

		curr := head
		for {
			next := curr.Prev.Load()
			if next == nil {
				break
			}

			xMax = next.XMax.Load()
			if xMax != tx.FROZEN_TX_ID && xMax != horizon && tx.Precedes(xMax, horizon) {
				curr.Prev.Store(nil)
				break
			}

			xMin = next.XMin.Load()
			if xMin != tx.FROZEN_TX_ID && xMin != horizon && tx.Precedes(xMin, horizon) {
				next.XMin.Store(tx.FROZEN_TX_ID)
			}

			curr = next
		}

		return true
	})
}
