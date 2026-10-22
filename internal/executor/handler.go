package executor

import "kv/internal/command"

type ExecutionResult struct {
	TxID  *uint64
	Value []byte
	Err   error
}

type TxControl interface {
	Begin() error
	Commit() error
	AbortTx() error
}

type Store interface {
	Set(key string, value []byte) error
	Get(key string) ([]byte, error)
	Delete(key string) error
}

type Handler func(tx TxControl, kv Store, cmd *command.Command) ExecutionResult

func HandlerByKeyword(keyword string) Handler {
	return handlersByKeyword[command.NormalizeKeyword(keyword)]
}

func RegisterHandler(keyword string, handler Handler) {
	handlersByKeyword[command.NormalizeKeyword(keyword)] = handler
}

func ResetHandlers() {
	handlersByKeyword = map[string]Handler{}
}

var handlersByKeyword = map[string]Handler{}
