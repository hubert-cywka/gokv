package mvcc

import (
	"errors"
	"kv/test"
	"sync"
	"sync/atomic"
	"testing"
)

func TestMVCC(t *testing.T) {
	txManager := setupTxManager()
	coordinator, _ := setup()

	givenEntryCommitted := func(key string, value []byte) {
		setupTx := beginTransaction(t, txManager)
		_ = coordinator.Set(key, value, setupTx)
		_ = setupTx.Commit()
	}

	t.Run("it rollbacks inserts when transaction is aborted", func(t *testing.T) {
		key := "rollbacks-inserts"
		givenEntryCommitted(key, []byte("100"))

		txA := beginTransaction(t, txManager)
		err := coordinator.Set(key, []byte("200"), txA)
		test.AssertNoError(t, err)

		txA.Abort()

		txB := beginTransaction(t, txManager)
		got, err := coordinator.Get(key, txB)
		_ = txB.Commit()

		test.AssertNoError(t, err)
		test.AssertBytesEqual(t, got, []byte("100"))
	})

	t.Run("it rollbacks deletes when transaction is aborted", func(t *testing.T) {
		key := "rollbacks-deletes"
		givenEntryCommitted(key, []byte("100"))

		txA := beginTransaction(t, txManager)
		err := coordinator.Delete(key, txA)
		test.AssertNoError(t, err)

		txA.Abort()

		txB := beginTransaction(t, txManager)
		got, err := coordinator.Get(key, txB)
		_ = txB.Commit()

		test.AssertNoError(t, err)
		test.AssertBytesEqual(t, got, []byte("100"))
	})

	t.Run("it rollbacks all changes when transaction is aborted", func(t *testing.T) {
		key1 := "rollbacks-all-changes-1"
		value1 := []byte("100")
		key2 := "rollbacks-all-changes-2"
		value2 := []byte("200")
		key3 := "rollbacks-all-changes-3"
		value3 := []byte("300")
		givenEntryCommitted(key3, value3)

		txA := beginTransaction(t, txManager)

		err := coordinator.Set(key1, value1, txA)
		test.AssertNoError(t, err)
		err = coordinator.Set(key1, value1, txA)
		test.AssertNoError(t, err)
		err = coordinator.Set(key1, value1, txA)
		test.AssertNoError(t, err)
		err = coordinator.Set(key2, value2, txA)
		test.AssertNoError(t, err)
		err = coordinator.Delete(key3, txA)
		test.AssertNoError(t, err)

		txA.Abort()

		txB := beginTransaction(t, txManager)
		_, err1 := coordinator.Get(key1, txB)
		_, err2 := coordinator.Get(key2, txB)
		got3, err3 := coordinator.Get(key3, txB)
		_ = txB.Commit()

		test.AssertError(t, err1, KeyNotFoundError)
		test.AssertError(t, err2, KeyNotFoundError)
		test.AssertNoError(t, err3)
		test.AssertBytesEqual(t, got3, value3)
	})

	t.Run("it allows set -> set in the same transaction", func(t *testing.T) {
		key := "set-to-set"
		value := []byte("100")

		txA := beginTransaction(t, txManager)

		err := coordinator.Set(key, value, txA)
		test.AssertNoError(t, err)
		err = coordinator.Set(key, value, txA)
		test.AssertNoError(t, err)

		_ = txA.Commit()
	})

	t.Run("it allows set -> delete -> set in the same transaction", func(t *testing.T) {
		key := "set-to-delete-to-set"
		value := []byte("100")

		txA := beginTransaction(t, txManager)

		err := coordinator.Set(key, value, txA)
		test.AssertNoError(t, err)
		err = coordinator.Delete(key, txA)
		test.AssertNoError(t, err)
		err = coordinator.Set(key, value, txA)
		test.AssertNoError(t, err)

		_ = txA.Commit()
	})

	t.Run("it ensures snapshot isolation", func(t *testing.T) {
		key := "versioned_key"

		v1 := []byte("v1")
		v2 := []byte("v2")
		v3 := []byte("v3")

		tx1 := beginTransaction(t, txManager)
		_ = coordinator.Set(key, v1, tx1)
		_ = tx1.Commit()

		snap1 := beginTransaction(t, txManager)

		tx2 := beginTransaction(t, txManager)
		_ = coordinator.Set(key, v2, tx2)
		_ = tx2.Commit()

		snap2 := beginTransaction(t, txManager)

		tx3 := beginTransaction(t, txManager)
		_ = coordinator.Set(key, v3, tx3)
		_ = tx3.Commit()

		got1, _ := coordinator.Get(key, snap1)
		test.AssertBytesEqual(t, got1, v1)

		got2, _ := coordinator.Get(key, snap2)
		test.AssertBytesEqual(t, got2, v2)
	})

	t.Run("it avoids data corruption in high concurrency scenario", func(t *testing.T) {
		key := "global_counter"

		setupTx := beginTransaction(t, txManager)
		_ = coordinator.Set(key, []byte("0"), setupTx)
		_ = setupTx.Commit()

		workers := 200
		iterations := 150
		var wg sync.WaitGroup

		var successfulUpdates atomic.Int64
		var serializationErrors atomic.Int64

		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < iterations; j++ {
					txA := beginTransaction(t, txManager)

					val, err := coordinator.Get(key, txA)
					test.AssertNoError(t, err)

					err = coordinator.Set(key, append(val, '+'), txA)

					if err == nil {
						if txA.Commit() == nil {
							successfulUpdates.Add(1)
						}
					} else if errors.Is(err, SerializationError) {
						serializationErrors.Add(1)
					}
				}
			}()
		}

		wg.Wait()

		finalTx := beginTransaction(t, txManager)
		finalVal, _ := coordinator.Get(key, finalTx)

		test.AssertEqual(t, len(finalVal)-1, int(successfulUpdates.Load()))
	})
}
