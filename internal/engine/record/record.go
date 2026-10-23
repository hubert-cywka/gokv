package record

const (
	Tombstone uint8 = iota
	Value
	Commit
	Freeze
)

type Record struct {
	TxID  uint64
	Kind  uint8
	Key   []byte
	Value []byte
}

func NewValue(key string, value []byte, txID uint64) *Record {
	return newRecord(Value, key, value, txID)
}

func NewTombstone(key string, txID uint64) *Record {
	return newRecord(Tombstone, key, nil, txID)
}

func NewCommit(txID uint64) *Record {
	return newRecord(Commit, "", nil, txID)
}

func NewFreeze(key string, txID uint64) *Record {
	return newRecord(Freeze, key, nil, txID)
}

func (r Record) Clone() Record {
	key := make([]byte, len(r.Key))
	copy(key, r.Key)

	value := make([]byte, len(r.Value))
	copy(value, r.Value)

	return Record{
		TxID:  r.TxID,
		Kind:  r.Kind,
		Key:   key,
		Value: value,
	}
}

func newRecord(kind uint8, key string, value []byte, txID uint64) *Record {
	return &Record{
		Kind:  kind,
		Key:   []byte(key),
		Value: value,
		TxID:  txID,
	}
}
