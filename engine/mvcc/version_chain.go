package mvcc

import (
	"kv/engine/tx"
	"sync/atomic"
)

type VersionChain struct {
	head atomic.Pointer[Version]
}

func NewVersionChain() *VersionChain {
	return &VersionChain{}
}

func (c *VersionChain) Head() *Version {
	return c.head.Load()
}

func (c *VersionChain) CompareHeadAndSwap(old *Version, new *Version) bool {
	return c.head.CompareAndSwap(old, new)
}

func (c *VersionChain) FindVisible(t *tx.Transaction) *Version {
	curr := c.head.Load()

	for curr != nil {
		if t.CanSee(curr.XMin(), curr.XMax()) {
			return curr
		}

		curr = curr.PreviousVersion()
	}

	return nil
}
