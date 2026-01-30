package cache

import (
	"sync"
)

type Cache struct {
	storage map[string][]byte
	mutex   sync.RWMutex
}

func NewCache(options Options) *Cache {
	return &Cache{
		storage: make(map[string][]byte, options.InitialCapacity),
		mutex:   sync.RWMutex{},
	}
}

func (c *Cache) Get(key string) ([]byte, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	value, ok := c.storage[key]
	if !ok {
		return nil, ErrNotFound
	}

	valCopy := copyBytes(value)
	return valCopy, nil
}

func (c *Cache) Set(key string, value []byte) error {
	valCopy := copyBytes(value)

	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.storage[key] = valCopy
	return nil
}

func (c *Cache) Delete(key string) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	delete(c.storage, key)
	return nil
}

func copyBytes(value []byte) []byte {
	valCopy := make([]byte, len(value))
	copy(valCopy, value)
	return valCopy
}
