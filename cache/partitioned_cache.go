package cache

import (
	"kv/hash"
)

type PartitionedCache struct {
	partitionsCount uint32
	partitions      []*Cache
}

func NewPartitionedCache(options Options) *PartitionedCache {
	cache := &PartitionedCache{
		partitionsCount: options.Partitions,
		partitions:      make([]*Cache, options.Partitions),
	}

	for i := range options.Partitions {
		cache.partitions[i] = NewCache(options)
	}

	return cache
}

func (pc *PartitionedCache) Get(key string) ([]byte, error) {
	partition := pc.findPartition(key)
	return partition.Get(key)
}

func (pc *PartitionedCache) Set(key string, value []byte) error {
	partition := pc.findPartition(key)
	return partition.Set(key, value)
}

func (pc *PartitionedCache) Delete(key string) error {
	partition := pc.findPartition(key)
	return partition.Delete(key)
}

func (pc *PartitionedCache) findPartition(key string) *Cache {
	index := hash.Fnv32a(key) % pc.partitionsCount
	return pc.partitions[index]
}
