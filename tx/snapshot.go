package tx

type Snapshot struct {
	xMin   uint64
	xMax   uint64
	active map[uint64]struct{}
}

func (s Snapshot) IsActive(id uint64) bool {
	_, ok := s.active[id]
	return ok
}
