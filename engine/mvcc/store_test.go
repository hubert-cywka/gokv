package mvcc

import (
	"kv/test"
	"testing"
)

func TestCoordinator_Get(t *testing.T) {
	txManager := setupTxManager()
	store, _ := setup()

	givenEntryCommitted := func(key string, value []byte) {
		setupTx := beginTransaction(t, txManager)
		_ = store.Set(key, value, setupTx)
		_ = setupTx.Commit()
	}

	t.Run("it returns value if key exists", func(t *testing.T) {
		key := "key-exists"
		initialValue := []byte("100")
		givenEntryCommitted(key, initialValue)

		got, err := store.Get(key, beginTransaction(t, txManager))

		test.AssertNoError(t, err)
		test.AssertBytesEqual(t, got, initialValue)
	})

	t.Run("it returns error if key does not exist", func(t *testing.T) {
		key := "key-does-not-exist"

		got, err := store.Get(key, beginTransaction(t, txManager))

		test.AssertError(t, err, KeyNotFoundError)
		test.AssertBytesEqual(t, got, nil)
	})

	t.Run("it handles dirty reads", func(t *testing.T) {
		key := "dirty-reads"
		initialValue := []byte("100")
		givenEntryCommitted(key, initialValue)

		txA := beginTransaction(t, txManager)

		txB := beginTransaction(t, txManager)
		_ = store.Set(key, []byte("200"), txB)
		_ = txB.Commit()

		got, err := store.Get(key, txA)

		test.AssertNoError(t, err)
		test.AssertBytesEqual(t, got, initialValue)
	})

	t.Run("it correctly skips multiple tombstones in version chain", func(t *testing.T) {
		key := "tombstone-skips"

		// Set v1 -> Commit -> Del -> Commit -> Loop x3
		for i := 1; i <= 3; i++ {
			setter := beginTransaction(t, txManager)
			_ = store.Set(key, []byte("val"), setter)
			_ = setter.Commit()

			deleter := beginTransaction(t, txManager)
			_ = store.Delete(key, deleter)
			_ = deleter.Commit()
		}

		value, err := store.Get(key, beginTransaction(t, txManager))

		test.AssertBytesEqual(t, value, nil)
		test.AssertError(t, err, KeyNotFoundError)
	})
}

func TestCoordinator_Set(t *testing.T) {
	txManager := setupTxManager()
	store, versionMap := setup()

	givenEntryCommitted := func(key string, value []byte) {
		setupTx := beginTransaction(t, txManager)
		_ = store.Set(key, value, setupTx)
		_ = setupTx.Commit()
	}

	t.Run("it sets new value visible in current transaction", func(t *testing.T) {
		key := "set-value-own-transaction"
		value := []byte("100")

		txA := beginTransaction(t, txManager)
		err := store.Set(key, value, txA)
		test.AssertNoError(t, err)

		chain, ok := versionMap.GetChain(key)
		test.AssertTrue(t, ok)

		got := chain.Head()
		test.AssertBytesEqual(t, got.Value, value)
	})

	t.Run("it sets new value visible in future transactions", func(t *testing.T) {
		key := "set-value-future-transactions"
		value := []byte("100")

		txA := beginTransaction(t, txManager)
		err := store.Set(key, value, txA)
		test.AssertNoError(t, err)

		err = txA.Commit()
		test.AssertNoError(t, err)

		chain, ok := versionMap.GetChain(key)
		test.AssertTrue(t, ok)

		got := chain.Head()
		AssertNotPruned(t, got)
		test.AssertBytesEqual(t, got.Value, value)
	})

	t.Run("it returns error when concurrent update is detected", func(t *testing.T) {
		key := "concurrent-updates"
		initialValue := []byte("1")
		givenEntryCommitted(key, initialValue)

		txA := beginTransaction(t, txManager)
		txB := beginTransaction(t, txManager)

		err := store.Set(key, []byte("2"), txA)
		test.AssertNoError(t, err)

		err = store.Set(key, []byte("3"), txB)
		test.AssertError(t, err, SerializationError)
	})

	t.Run("it prevents deadlocks when inserting", func(t *testing.T) {
		key1 := "deadlock-1-insert"
		key2 := "deadlock-2-insert"
		value := []byte("1")

		txA := beginTransaction(t, txManager)
		txB := beginTransaction(t, txManager)

		err := store.Set(key1, value, txA)
		test.AssertNoError(t, err)

		err = store.Set(key2, value, txB)
		test.AssertNoError(t, err)

		err = store.Set(key2, value, txA)
		test.AssertError(t, err, SerializationError)

		err = store.Set(key1, value, txB)
		test.AssertError(t, err, SerializationError)
	})

	t.Run("it prevents deadlocks when updating", func(t *testing.T) {
		key1 := "deadlock-1-update"
		key2 := "deadlock-2-update"
		value := []byte("1")
		givenEntryCommitted(key1, value)
		givenEntryCommitted(key2, value)

		txA := beginTransaction(t, txManager)
		txB := beginTransaction(t, txManager)

		err := store.Set(key1, value, txA)
		test.AssertNoError(t, err)

		err = store.Set(key2, value, txB)
		test.AssertNoError(t, err)

		err = store.Set(key2, value, txA)
		test.AssertError(t, err, SerializationError)

		err = store.Set(key1, value, txB)
		test.AssertError(t, err, SerializationError)
	})
}

func TestCoordinator_Delete(t *testing.T) {
	txManager := setupTxManager()
	vacuumer, versionMap := setup()

	givenEntryCommitted := func(key string, value []byte) {
		setupTx := beginTransaction(t, txManager)
		_ = vacuumer.Set(key, value, setupTx)
		_ = setupTx.Commit()
	}

	t.Run("it marks entry as deleted in current transaction", func(t *testing.T) {
		key := "delete-existing"
		givenEntryCommitted(key, []byte("100"))

		txA := beginTransaction(t, txManager)
		err := vacuumer.Delete(key, txA)
		test.AssertNoError(t, err)

		chain, ok := versionMap.GetChain(key)
		test.AssertTrue(t, ok)

		got := chain.Head()
		test.AssertEqual(t, got.XMax(), txA.ID)
	})

	t.Run("it returns error when key does not exist", func(t *testing.T) {
		txA := beginTransaction(t, txManager)
		err := vacuumer.Delete("non-existent", txA)

		test.AssertError(t, err, KeyNotFoundError)
	})

	t.Run("it returns error when entry is already being deleted by another transaction", func(t *testing.T) {
		key := "concurrent-delete"
		givenEntryCommitted(key, []byte("val"))

		txA := beginTransaction(t, txManager)
		txB := beginTransaction(t, txManager)

		err := vacuumer.Delete(key, txA)
		test.AssertNoError(t, err)

		err = vacuumer.Delete(key, txB)
		test.AssertError(t, err, SerializationError)
	})

	t.Run("it returns error when trying to delete entry created by future transaction", func(t *testing.T) {
		key := "visibility-conflict"

		txA := beginTransaction(t, txManager)

		givenEntryCommitted(key, []byte("future-val"))

		err := vacuumer.Delete(key, txA)
		test.AssertError(t, err, SerializationError)
	})

	t.Run("it prevents deadlocks", func(t *testing.T) {
		key1 := "deadlock-1-delete"
		key2 := "deadlock-2-delete"
		value := []byte("1")
		givenEntryCommitted(key1, value)
		givenEntryCommitted(key2, value)

		txA := beginTransaction(t, txManager)
		txB := beginTransaction(t, txManager)

		err := vacuumer.Delete(key1, txA)
		test.AssertNoError(t, err)

		err = vacuumer.Delete(key2, txB)
		test.AssertNoError(t, err)

		err = vacuumer.Delete(key2, txA)
		test.AssertError(t, err, SerializationError)

		err = vacuumer.Delete(key1, txB)
		test.AssertError(t, err, SerializationError)
	})
}
