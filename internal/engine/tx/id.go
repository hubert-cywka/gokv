package tx

const IdFrozen = ID(0)
const IdAlive = ID(1)

type ID uint64

func (txID ID) Precedes(candidate ID) bool {
	return candidate-txID < HalfSpace
}

func (txID ID) IsFrozen() bool {
	return txID == IdFrozen
}

func (txID ID) IsAlive() bool {
	return txID == IdAlive
}

func (txID ID) IsReserved() bool {
	return txID.IsFrozen() || txID.IsAlive()
}

func (txID ID) Uint64() uint64 {
	return uint64(txID)
}
