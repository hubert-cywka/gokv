package kvstore

import (
	"kv/engine/tx"
)

type Options struct {
	Validation ValidationOptions
}

type ValidationOptions struct {
	MaxKeySize   int
	MaxValueSize int
}

type KVStore struct {
	store   Store
	options Options
}

func New(store Store, options Options) *KVStore {
	return &KVStore{
		store:   store,
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
