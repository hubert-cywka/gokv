package memstore

import (
	"kv/memstore/entry"
	"kv/tx"
	"sync"
	"sync/atomic"
)

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
		xMin := curr.XMin()
		xMax := curr.XMax()

		if t.CanSee(xMin, xMax) {
			if curr.Value == nil {
				return nil, KeyNotFoundError
			}

			return curr.Value, nil
		}

		curr = curr.Prev()
	}

	return nil, KeyNotFoundError
}

func (s *MemStore) Set(key string, value []byte, t *tx.Transaction) error {
	actual, _ := s.storage.LoadOrStore(key, &atomic.Pointer[entry.Entry]{})
	ptr := actual.(*atomic.Pointer[entry.Entry])

	for {
		latest := ptr.Load()

		// Allow update if entry was deleted by this transaction
		if latest != nil && latest.XMax() != t.ID {
			if latest.XMax() != tx.FROZEN_TX_ID {
				return SerializationError
			}

			if !t.CanSee(latest.XMin(), tx.FROZEN_TX_ID) {
				return SerializationError
			}

			if !latest.CompareAndSetXMax(tx.FROZEN_TX_ID, t.ID) {
				return SerializationError
			}
		}

		newEntry := entry.New(key, value, t.ID)
		newEntry.SetPrev(latest)

		if ptr.CompareAndSwap(latest, &newEntry) {
			t.Track(latest)
			t.Track(&newEntry)
			return nil
		}

		if latest != nil {
			latest.SetXMax(tx.FROZEN_TX_ID)
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

		if latest.XMax() != tx.FROZEN_TX_ID {
			return SerializationError
		}

		if !t.CanSee(latest.XMin(), tx.FROZEN_TX_ID) {
			return SerializationError
		}

		if !latest.CompareAndSetXMax(tx.FROZEN_TX_ID, t.ID) {
			return SerializationError
		}

		t.Track(latest)
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

		xMax := head.XMax()
		if xMax != tx.FROZEN_TX_ID && xMax != horizon && tx.Precedes(xMax, horizon) {
			ptr.CompareAndSwap(head, nil)
			return true
		}

		xMin := head.XMin()
		if xMin != tx.FROZEN_TX_ID && xMin != horizon && tx.Precedes(xMin, horizon) {
			head.SetXMin(tx.FROZEN_TX_ID)
		}

		curr := head
		for {
			next := curr.Prev()
			if next == nil {
				break
			}

			xMax = next.XMax()
			if xMax != tx.FROZEN_TX_ID && xMax != horizon && tx.Precedes(xMax, horizon) {
				curr.SetPrev(nil)
				break
			}

			xMin = next.XMin()
			if xMin != tx.FROZEN_TX_ID && xMin != horizon && tx.Precedes(xMin, horizon) {
				next.SetXMin(tx.FROZEN_TX_ID)
			}

			curr = next
		}

		return true
	})
}
