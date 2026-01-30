package cache

import (
	"fmt"
	"testing"
)

func BenchmarkCache(b *testing.B) {
	options := Options{
		InitialCapacity: 512,
	}

	c := NewCache(options)
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key-%d", i)
			_ = c.Set(key, []byte("value"))
			_, _ = c.Get(key)
			i++
		}
	})
}

func BenchmarkPartitionedCache(b *testing.B) {
	options := Options{
		InitialCapacity: 512,
		Partitions:      128,
	}

	pc := NewPartitionedCache(options)
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key-%d", i)
			_ = pc.Set(key, []byte("value"))
			_, _ = pc.Get(key)
			i++
		}
	})
}
