package main

import (
	"kv/wal"
	"kv/wal/data"

	"github.com/rs/zerolog/log"
)

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

func (s *KeyValueStore) Get(key string) ([]byte, error) {
	err := s.validateKey(key)

	if err != nil {
		return nil, err
	}

	value, err := s.store.Get(key)
	return value, err
}

func (s *KeyValueStore) Set(key string, value []byte) error {
	err := s.validateKey(key)

	if err != nil {
		return err
	}

	err = s.validateValue(value)

	if err != nil {
		return err
	}

	record := data.NewValueRecord(key, value)
	walErr := s.wal.Append(record)

	if walErr != nil {
		return walErr
	}

	storeErr := s.store.Set(key, value)

	if storeErr != nil {
		log.Panic().
			Err(storeErr).
			Msg("kvstore: in-memory store got out-of-sync")
	}

	return nil
}

func (s *KeyValueStore) Delete(key string) error {
	err := s.validateKey(key)

	if err != nil {
		return err
	}

	record := data.NewTombstoneRecord(key)
	walErr := s.wal.Append(record)

	if walErr != nil {
		return walErr
	}

	storeErr := s.store.Delete(key)

	if storeErr != nil {
		log.Panic().
			Err(storeErr).
			Msg("kvstore: in-memory store got out-of-sync")
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
