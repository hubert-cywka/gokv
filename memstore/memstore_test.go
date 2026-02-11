package memstore

import (
	"errors"
	"kv/memstore/entry"
	"kv/test"
	"kv/tx"
	"sync"
	"sync/atomic"
	"testing"
)

func TestMemStore_Get(t *testing.T) {
	tm := tx.NewTransactionManager()
	storage := &sync.Map{}
	ms := New(storage)

	givenEntryCommitted := func(key string, value []byte) {
		setupTx := tm.Begin()
		_ = ms.Set(key, value, setupTx)
		_ = setupTx.Commit()
	}

	t.Run("it returns value if key exists", func(t *testing.T) {
		key := "key-exists"
		initialValue := []byte("100")
		givenEntryCommitted(key, initialValue)

		got, err := ms.Get(key, tm.Begin())

		test.AssertNoError(t, err)
		test.AssertBytesEqual(t, got, initialValue)
	})

	t.Run("it returns error if key does not exist", func(t *testing.T) {
		key := "key-does-not-exist"

		got, err := ms.Get(key, tm.Begin())

		test.AssertError(t, err, KeyNotFoundError)
		test.AssertBytesEqual(t, got, nil)
	})

	t.Run("it handles dirty reads", func(t *testing.T) {
		key := "dirty-reads"
		initialValue := []byte("100")
		givenEntryCommitted(key, initialValue)

		txA := tm.Begin()

		txB := tm.Begin()
		_ = ms.Set(key, []byte("200"), txB)
		_ = txB.Commit()

		got, err := ms.Get(key, txA)

		test.AssertNoError(t, err)
		test.AssertBytesEqual(t, got, initialValue)
	})

	t.Run("it correctly skips multiple tombstones in version chain", func(t *testing.T) {
		key := "tombstone-skips"

		// Set v1 -> Commit -> Del -> Commit -> Loop x3
		for i := 1; i <= 3; i++ {
			setter := tm.Begin()
			_ = ms.Set(key, []byte("val"), setter)
			_ = setter.Commit()

			deleter := tm.Begin()
			_ = ms.Delete(key, deleter)
			_ = deleter.Commit()
		}

		value, err := ms.Get(key, tm.Begin())

		test.AssertBytesEqual(t, value, nil)
		test.AssertError(t, err, KeyNotFoundError)
	})
}

func TestMemStore_Set(t *testing.T) {
	tm := tx.NewTransactionManager()
	storage := &sync.Map{}
	ms := New(storage)

	givenEntryCommitted := func(key string, value []byte) {
		setupTx := tm.Begin()
		_ = ms.Set(key, value, setupTx)
		_ = setupTx.Commit()
	}

	t.Run("it sets new value visible in current transaction", func(t *testing.T) {
		key := "set-value-own-transaction"
		value := []byte("100")

		txA := tm.Begin()
		err := ms.Set(key, value, txA)
		test.AssertNoError(t, err)

		entries := introspect(storage)
		got := findEntry(entries, key)
		assertNotPruned(t, got)
		test.AssertBytesEqual(t, got.Value, value)
	})

	t.Run("it sets new value visible in future transactions", func(t *testing.T) {
		key := "set-value-future-transactions"
		value := []byte("100")

		txA := tm.Begin()
		err := ms.Set(key, value, txA)
		test.AssertNoError(t, err)

		err = txA.Commit()
		test.AssertNoError(t, err)

		entries := introspect(storage)
		got := findEntry(entries, key)
		assertNotPruned(t, got)
		test.AssertBytesEqual(t, got.Value, value)
	})

	t.Run("it returns error when concurrent update is detected", func(t *testing.T) {
		key := "concurrent-updates"
		initialValue := []byte("1")
		givenEntryCommitted(key, initialValue)

		txA := tm.Begin()
		txB := tm.Begin()

		err := ms.Set(key, []byte("2"), txA)
		test.AssertNoError(t, err)

		err = ms.Set(key, []byte("3"), txB)
		test.AssertError(t, err, SerializationError)
	})

	t.Run("it prevents deadlocks when inserting", func(t *testing.T) {
		key1 := "deadlock-1-insert"
		key2 := "deadlock-2-insert"
		value := []byte("1")

		txA := tm.Begin()
		txB := tm.Begin()

		err := ms.Set(key1, value, txA)
		test.AssertNoError(t, err)

		err = ms.Set(key2, value, txB)
		test.AssertNoError(t, err)

		err = ms.Set(key2, value, txA)
		test.AssertError(t, err, SerializationError)

		err = ms.Set(key1, value, txB)
		test.AssertError(t, err, SerializationError)
	})

	t.Run("it prevents deadlocks when updating", func(t *testing.T) {
		key1 := "deadlock-1-update"
		key2 := "deadlock-2-update"
		value := []byte("1")
		givenEntryCommitted(key1, value)
		givenEntryCommitted(key2, value)

		txA := tm.Begin()
		txB := tm.Begin()

		err := ms.Set(key1, value, txA)
		test.AssertNoError(t, err)

		err = ms.Set(key2, value, txB)
		test.AssertNoError(t, err)

		err = ms.Set(key2, value, txA)
		test.AssertError(t, err, SerializationError)

		err = ms.Set(key1, value, txB)
		test.AssertError(t, err, SerializationError)
	})
}

func TestMemStore_Delete(t *testing.T) {
	tm := tx.NewTransactionManager()
	storage := &sync.Map{}
	ms := New(storage)

	givenEntryCommitted := func(key string, value []byte) {
		setupTx := tm.Begin()
		_ = ms.Set(key, value, setupTx)
		_ = setupTx.Commit()
	}

	t.Run("it marks entry as deleted in current transaction", func(t *testing.T) {
		key := "delete-existing"
		givenEntryCommitted(key, []byte("100"))

		txA := tm.Begin()
		err := ms.Delete(key, txA)
		test.AssertNoError(t, err)

		entries := introspect(storage)
		got := findEntry(entries, key)
		test.AssertEqual(t, got.XMax.Load(), txA.ID)
	})

	t.Run("it returns error when key does not exist", func(t *testing.T) {
		txA := tm.Begin()
		err := ms.Delete("non-existent", txA)

		test.AssertError(t, err, KeyNotFoundError)
	})

	t.Run("it returns error when entry is already being deleted by another transaction", func(t *testing.T) {
		key := "concurrent-delete"
		givenEntryCommitted(key, []byte("val"))

		txA := tm.Begin()
		txB := tm.Begin()

		err := ms.Delete(key, txA)
		test.AssertNoError(t, err)

		err = ms.Delete(key, txB)
		test.AssertError(t, err, SerializationError)
	})

	t.Run("it returns error when trying to delete entry created by future transaction", func(t *testing.T) {
		key := "visibility-conflict"

		txA := tm.Begin()

		givenEntryCommitted(key, []byte("future-val"))

		err := ms.Delete(key, txA)
		test.AssertError(t, err, SerializationError)
	})

	t.Run("it prevents deadlocks", func(t *testing.T) {
		key1 := "deadlock-1-delete"
		key2 := "deadlock-2-delete"
		value := []byte("1")
		givenEntryCommitted(key1, value)
		givenEntryCommitted(key2, value)

		txA := tm.Begin()
		txB := tm.Begin()

		err := ms.Delete(key1, txA)
		test.AssertNoError(t, err)

		err = ms.Delete(key2, txB)
		test.AssertNoError(t, err)

		err = ms.Delete(key2, txA)
		test.AssertError(t, err, SerializationError)

		err = ms.Delete(key1, txB)
		test.AssertError(t, err, SerializationError)
	})
}

func TestMemStore_Vacuum(t *testing.T) {
	tm := tx.NewTransactionManager()
	storage := &sync.Map{}
	ms := New(storage)

	givenEntryCommitted := func(key string, value []byte) {
		setupTx := tm.Begin()
		_ = ms.Set(key, value, setupTx)
		_ = setupTx.Commit()
	}

	givenEntryDeleted := func(key string) {
		setupTx := tm.Begin()
		_ = ms.Delete(key, setupTx)
		_ = setupTx.Commit()
	}

	givenTransactionActive := func() *tx.Transaction {
		return tm.Begin()
	}

	t.Run("it keeps latest version when no transactions are active", func(t *testing.T) {
		key := "keep-no-transactions"
		initialValue := []byte("111")
		givenEntryCommitted(key, initialValue)

		ms.Vacuum(tm)

		entries := introspect(storage)
		got := findEntry(entries, key)

		assertNotPruned(t, got)
	})

	t.Run("it keeps latest version when older transactions are still active", func(t *testing.T) {
		key := "keep-older-transactions"
		initialValue := []byte("111")
		givenEntryCommitted(key, initialValue)
		txA := givenTransactionActive()

		ms.Vacuum(tm)
		_ = txA.Commit()

		entries := introspect(storage)
		got := findEntry(entries, key)

		assertNotPruned(t, got)
	})

	t.Run("it removes older versions when no transactions are active", func(t *testing.T) {
		key := "remove-older-no-transactions"
		initialValue := []byte("111")
		givenEntryCommitted(key, initialValue)
		givenEntryCommitted(key, initialValue)

		ms.Vacuum(tm)

		entries := introspect(storage)
		got := findEntry(entries, key)

		assertNotPruned(t, got)
		assertPruned(t, got.Prev.Load())
	})

	t.Run("it freezes latest version when no transactions are active", func(t *testing.T) {
		key := "freeze-latest-no-transactions"
		initialValue := []byte("111")
		givenEntryCommitted(key, initialValue)
		givenEntryCommitted(key, initialValue)

		ms.Vacuum(tm)

		entries := introspect(storage)
		got := findEntry(entries, key)

		assertFrozen(t, got)
	})

	t.Run("it removes older versions when they are not visible by any transaction", func(t *testing.T) {
		key := "remove-older-not-visible-by-other-transactions"
		initialValue := []byte("111")
		givenEntryCommitted(key, initialValue)
		givenEntryCommitted(key, initialValue)
		txA := givenTransactionActive()

		ms.Vacuum(tm)
		_ = txA.Commit()

		entries := introspect(storage)
		got := findEntry(entries, key)

		assertNotPruned(t, got)
		assertPruned(t, got.Prev.Load())
	})

	t.Run("it freezes version that is visible by all transactions", func(t *testing.T) {
		key := "freeze-visible-by-all-transactions"
		initialValue := []byte("111")
		givenEntryCommitted(key, initialValue)
		givenEntryCommitted(key, initialValue)
		txA := givenTransactionActive()

		ms.Vacuum(tm)
		_ = txA.Commit()

		entries := introspect(storage)
		got := findEntry(entries, key)

		assertFrozen(t, got)
	})

	t.Run("it does not remove older versions when they are still visible by other transactions", func(t *testing.T) {
		key := "keeps-older-versions-still-visible-by-other-transactions"
		initialValue := []byte("111")
		givenEntryCommitted(key, initialValue)
		txA := givenTransactionActive()
		givenEntryCommitted(key, initialValue)

		ms.Vacuum(tm)
		_ = txA.Commit()

		entries := introspect(storage)
		got := findEntry(entries, key)

		assertNotPruned(t, got)
		assertNotPruned(t, got.Prev.Load())
	})

	t.Run("it does not freeze versions that are not visible by all transactions", func(t *testing.T) {
		key := "does-not-freeze-versions-not-visible-by-other-transactions"
		initialValue := []byte("111")
		givenEntryCommitted(key, initialValue)
		txA := givenTransactionActive()
		givenEntryCommitted(key, initialValue)

		ms.Vacuum(tm)
		_ = txA.Commit()

		entries := introspect(storage)
		got := findEntry(entries, key)

		assertNotFrozen(t, got)
	})

	t.Run("it removes tombstone not visible to any transaction", func(t *testing.T) {
		key := "delete-invisible"
		givenEntryCommitted(key, []byte("v1"))
		givenEntryDeleted(key)

		entries := introspect(storage)
		ms.Vacuum(tm)

		entries = introspect(storage)
		got := findEntry(entries, key)

		assertPruned(t, got)
	})

	t.Run("it prunes older versions in a long chain", func(t *testing.T) {
		key := "chain-of-versions"
		givenEntryCommitted(key, []byte("v1"))
		givenEntryCommitted(key, []byte("v2"))
		givenEntryCommitted(key, []byte("v3"))
		givenEntryCommitted(key, []byte("v4"))
		givenEntryCommitted(key, []byte("v5"))

		ms.Vacuum(tm)

		entries := introspect(storage)
		got := findEntry(entries, key)

		assertNotPruned(t, got)
		assertPruned(t, got.Prev.Load())
	})

	t.Run("it does not remove uncommitted versions", func(t *testing.T) {
		key := "does-not-remove-uncommitted"
		givenEntryCommitted(key, []byte("v1"))
		txLive := tm.Begin()
		_ = ms.Set(key, []byte("v2"), txLive)

		ms.Vacuum(tm)

		entries := introspect(storage)
		got := findEntry(entries, key)

		assertNotPruned(t, got)
		_ = txLive.Commit()
	})

	t.Run("it does not freeze uncommitted versions", func(t *testing.T) {
		key := "does-not-freeze-uncommitted"
		givenEntryCommitted(key, []byte("v1"))
		txLive := tm.Begin()
		_ = ms.Set(key, []byte("v2"), txLive)

		ms.Vacuum(tm)

		entries := introspect(storage)
		got := findEntry(entries, key)

		assertNotFrozen(t, got)
		_ = txLive.Commit()
	})

	t.Run("it does not remove older version, when newer version is still uncommitted", func(t *testing.T) {
		key := "live-uncommitted"
		givenEntryCommitted(key, []byte("v1"))
		txLive := tm.Begin()
		_ = ms.Set(key, []byte("v2"), txLive)

		ms.Vacuum(tm)

		entries := introspect(storage)
		got := findEntry(entries, key)

		assertNotPruned(t, got)
		assertNotPruned(t, got.Prev.Load())
		_ = txLive.Commit()
	})
}

func TestMemStore(t *testing.T) {
	t.Run("it ensures snapshot isolation", func(t *testing.T) {
		tm := tx.NewTransactionManager()
		storage := &sync.Map{}
		ms := New(storage)

		key := "versioned_key"

		v1 := []byte("v1")
		v2 := []byte("v2")
		v3 := []byte("v3")

		tx1 := tm.Begin()
		_ = ms.Set(key, v1, tx1)
		_ = tx1.Commit()

		snap1 := tm.Begin()

		tx2 := tm.Begin()
		_ = ms.Set(key, v2, tx2)
		_ = tx2.Commit()

		snap2 := tm.Begin()

		tx3 := tm.Begin()
		_ = ms.Set(key, v3, tx3)
		_ = tx3.Commit()

		got1, _ := ms.Get(key, snap1)
		test.AssertBytesEqual(t, got1, v1)

		got2, _ := ms.Get(key, snap2)
		test.AssertBytesEqual(t, got2, v2)
	})

	t.Run("it avoids data corruption in high concurrency scenario", func(t *testing.T) {
		tm := tx.NewTransactionManager()
		storage := &sync.Map{}
		ms := New(storage)

		key := "global_counter"

		setupTx := tm.Begin()
		_ = ms.Set(key, []byte("0"), setupTx)
		_ = setupTx.Commit()

		workers := 200
		iterations := 50
		var wg sync.WaitGroup

		var successfulUpdates atomic.Int64
		var serializationErrors atomic.Int64

		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < iterations; j++ {
					txA := tm.Begin()

					val, err := ms.Get(key, txA)
					test.AssertNoError(t, err)

					err = ms.Set(key, append(val, '+'), txA)

					if err == nil {
						if txA.Commit() == nil {
							successfulUpdates.Add(1)
						}
					} else if errors.Is(err, SerializationError) {
						serializationErrors.Add(1)
					}
				}
			}()

			go func() {
				ms.Vacuum(tm)
			}()
		}

		wg.Wait()

		finalTx := tm.Begin()
		finalVal, _ := ms.Get(key, finalTx)

		test.AssertEqual(t, len(finalVal)-1, int(successfulUpdates.Load()))
	})
}

func assertFrozen(t *testing.T, e *entry.Entry) {
	t.Helper()

	test.AssertEqual(t, e.XMin.Load(), tx.FROZEN_TX_ID)
}

func assertNotFrozen(t *testing.T, e *entry.Entry) {
	t.Helper()

	test.AssertNotEqual(t, e.XMin.Load(), tx.FROZEN_TX_ID)
}

func assertNotPruned(t *testing.T, e *entry.Entry) {
	t.Helper()

	test.AssertNotEqual(t, e, nil)
}

func assertPruned(t *testing.T, e *entry.Entry) {
	t.Helper()

	test.AssertEqual(t, e, nil)
}

func findEntry(entries []*entry.Entry, key string) *entry.Entry {
	for _, e := range entries {
		if e.Key == key {
			return e
		}
	}

	return nil
}

func introspect(s *sync.Map) []*entry.Entry {
	var result []*entry.Entry

	s.Range(func(_, val interface{}) bool {
		ptr := val.(*atomic.Pointer[entry.Entry])
		e := ptr.Load()

		if e != nil {
			result = append(result, e)
		}

		return true
	})

	return result
}
