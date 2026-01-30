package kv_store

import (
	"kv/store"
	"kv/wal"
)

type KeyValueStore struct {
	store store.Store
	wal   wal.Log
}

func NewKeyValueStore(store store.Store, wal wal.Log) *KeyValueStore {
	return &KeyValueStore{
		store: store,
		wal:   wal,
	}
}

func (s *KeyValueStore) Get(key string) ([]byte, error) {
	value, err := s.store.Get(key)
	return value, err
}

func (s *KeyValueStore) Set(key string, value []byte) error {
	record := wal.NewRecord(key, value)
	walErr := s.wal.Append(record)

	if walErr != nil {
		return walErr
	}

	storeErr := s.store.Set(key, value)

	if storeErr != nil {
		return storeErr
	}

	return nil
}

func (s *KeyValueStore) Delete(key string) error {
	record := wal.NewTombstone(key)
	walErr := s.wal.Append(record)

	if walErr != nil {
		return walErr
	}

	storeErr := s.store.Delete(key)

	if storeErr != nil {
		return storeErr
	}

	return nil
}
