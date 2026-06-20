package command

type ExecutionResult struct {
	TxID  *uint64
	Value []byte
	Err   error
}

type TxControl interface {
	CurrentTxID() *uint64
	Begin() error
	Commit() error
	AbortTx() error
}

type Store interface {
	Set(key string, value []byte) error
	Get(key string) ([]byte, error)
	Delete(key string) error
}

type Handler func(tx TxControl, kv Store, cmd *Command) ExecutionResult
