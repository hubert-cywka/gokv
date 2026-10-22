package mvcc

import (
	"sync"
)

type VersionMap struct {
	data *sync.Map
}

func NewVersionMap() *VersionMap {
	return &VersionMap{
		data: &sync.Map{},
	}
}

func (vm *VersionMap) GetChain(key string) (*VersionChain, bool) {
	val, ok := vm.data.Load(key)

	if !ok {
		return nil, false
	}

	return val.(*VersionChain), true
}

func (vm *VersionMap) GetOrCreateChain(key string) *VersionChain {
	actual, _ := vm.data.LoadOrStore(key, NewVersionChain())
	return actual.(*VersionChain)
}

func (vm *VersionMap) Range(fn func(key string, chain *VersionChain) bool) {
	vm.data.Range(func(k, v any) bool {
		return fn(k.(string), v.(*VersionChain))
	})
}

func (vm *VersionMap) Remove(key string) {
	vm.data.Delete(key)
}

func (vm *VersionMap) Set(key string, chain *VersionChain) {
	vm.data.Store(key, chain)
}
