package tx

type Snapshot struct {
	xMin   ID
	xMax   ID
	active map[ID]struct{}
}

func newSnapshot(xMin, xMax ID, active map[ID]struct{}) Snapshot {
	return Snapshot{
		xMin:   xMin,
		xMax:   xMax,
		active: active,
	}
}

func (s Snapshot) IsActive(id ID) bool {
	_, ok := s.active[id]
	return ok
}
