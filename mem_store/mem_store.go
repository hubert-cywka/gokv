package mem_store

import (
	"sync"
)

type MemStore struct {
	storage map[string][]byte
	mutex   sync.RWMutex
}

func (s *MemStore) Set(key string, value []byte) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.storage[key] = value

	return nil
}

func (s *MemStore) Get(key string) ([]byte, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	value, ok := s.storage[key]

	if !ok {
		return nil, ErrNotFound
	}

	return value, nil
}

func (s *MemStore) Delete(key string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	delete(s.storage, key)

	return nil
}

func NewMemStore() *MemStore {
	return &MemStore{
		storage: make(map[string][]byte),
		mutex:   sync.RWMutex{},
	}
}
