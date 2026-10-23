package application

import (
	"kv/internal/command"
)

type ExecutionResult struct {
	TxID  *uint64
	Value []byte
	Err   error
}

type Handler func(session *Session, cmd *command.Command) ExecutionResult

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
