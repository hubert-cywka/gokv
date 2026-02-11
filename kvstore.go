package main

import (
	"kv/tx"
	"kv/wal"
	"kv/wal/record"
)

/*
1. BEGIN TRANSACTION
   1.1 Unique, strict increasing ID is assigned.
   1.2 Snapshot of active transactions is created.

2. SELECT
   2.1 Do not acquire any locks. Do not wait for any locks.
   2.2 Return last visible version.

3. UPDATE / DELETE
   - Acquire row lock (wait if necessary). Watch out for deadlocks.
   - Find last version visible for T1.
   - Check if this version was modified by any other transaction (compare xmax to snapshot).
     - If it was modified, throw serializable error.
     - If it was not modified, create new version.

4. COMMIT
   4.1 Save data in WAL (INSERT/DELETE record + COMMIT record).
   4.2 Mark T1 as completed. New transactions will see data committed by T1.

5. ROLLBACK
   5.1 T1 was never committed, so data modified by T1 is ignored.
   5.2 Locks (if still taken) are released.
*/

type KeyValueStoreOptions struct {
	Validation ValidationOptions
}

type ValidationOptions struct {
	MaxKeySize   int
	MaxValueSize int
}

type KeyValueStore struct {
	store   Store
	wal     *wal.WriteAheadLog
	options KeyValueStoreOptions
}

func NewKeyValueStore(store Store, wal *wal.WriteAheadLog, options KeyValueStoreOptions) *KeyValueStore {
	return &KeyValueStore{
		store:   store,
		wal:     wal,
		options: options,
	}
}

func (s *KeyValueStore) Get(key string, transaction *tx.Transaction) ([]byte, error) {
	if err := s.validateKey(key); err != nil {
		return nil, err
	}

	value, err := s.store.Get(key, transaction)
	return value, err
}

func (s *KeyValueStore) Set(key string, value []byte, transaction *tx.Transaction) error {
	if err := s.validateKey(key); err != nil {
		return err
	}

	if err := s.validateValue(value); err != nil {
		return err
	}

	if err := s.store.Set(key, value, transaction); err != nil {
		return err
	}

	if err := s.wal.Append(record.NewValue(key, value, transaction.ID)); err != nil {
		transaction.Abort()
		return err
	}

	return nil
}

func (s *KeyValueStore) Delete(key string, transaction *tx.Transaction) error {
	if err := s.validateKey(key); err != nil {
		return err
	}

	if err := s.store.Delete(key, transaction); err != nil {
		return err
	}

	if err := s.wal.Append(record.NewTombstone(key, transaction.ID)); err != nil {
		transaction.Abort()
		return err
	}

	return nil
}

func (s *KeyValueStore) validateKey(key string) error {
	if len(key) > s.options.Validation.MaxKeySize {
		return ErrKeyTooLong
	}

	return nil
}

func (s *KeyValueStore) validateValue(value []byte) error {
	if len(value) > s.options.Validation.MaxValueSize {
		return ErrValueTooLong
	}

	return nil
}
