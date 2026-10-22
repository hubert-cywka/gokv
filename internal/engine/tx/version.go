package tx

type version interface {
	XMin() ID
	XMax() ID
	Freeze()
	Resurrect()
	TryKill(x ID) (ok bool)
}
