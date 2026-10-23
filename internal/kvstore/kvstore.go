package kvstore

import (
	"kv/internal/engine/mvcc"
	"kv/internal/engine/tx"
	"kv/internal/engine/wal"
	"kv/internal/engine/wal/record"
)

type Options struct {
	Validation ValidationOptions
}

type ValidationOptions struct {
	MaxKeySize   int
	MaxValueSize int
}

type KVStore struct {
	store   *mvcc.Store
	wal     wal.Appender
	options Options
}

func New(store *mvcc.Store, walAppender wal.Appender, options Options) *KVStore {
	return &KVStore{
		store:   store,
		wal:     walAppender,
		options: options,
	}
}

func (s *KVStore) Get(key string, transaction *tx.Transaction) ([]byte, error) {
	if err := s.validateKey(key); err != nil {
		return nil, err
	}

	value, err := s.store.Get(key, transaction)
	return value, err
}

func (s *KVStore) Set(key string, value []byte, transaction *tx.Transaction) error {
	if err := s.validateKey(key); err != nil {
		return err
	}

	if err := s.validateValue(value); err != nil {
		return err
	}

	if err := s.store.Set(key, value, transaction); err != nil {
		transaction.Abort()
		return err
	}

	valueRecord := record.NewValue(key, value, transaction.ID.Uint64())
	if err := s.wal.Append(valueRecord); err != nil {
		transaction.Abort()
		return err
	}

	return nil
}

func (s *KVStore) Delete(key string, transaction *tx.Transaction) error {
	if err := s.validateKey(key); err != nil {
		return err
	}

	if err := s.store.Delete(key, transaction); err != nil {
		transaction.Abort()
		return err
	}

	tombstoneRecord := record.NewTombstone(key, transaction.ID.Uint64())
	if err := s.wal.Append(tombstoneRecord); err != nil {
		transaction.Abort()
		return err
	}

	return nil
}

func (s *KVStore) validateKey(key string) error {
	if len(key) > s.options.Validation.MaxKeySize {
		return ErrKeyTooLong
	}

	return nil
}

func (s *KVStore) validateValue(value []byte) error {
	if len(value) > s.options.Validation.MaxValueSize {
		return ErrValueTooLong
	}

	return nil
}
