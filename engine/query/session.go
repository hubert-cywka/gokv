package query

import (
	"errors"
	"kv/command"
	"kv/engine/tx"
	"kv/kvstore"
)

type Session struct {
	txManager *tx.Manager
	kvStore   *kvstore.KVStore

	currentTx   *tx.Transaction
	currentTxID *uint64
}

var (
	ErrOperationOutsideTransaction = errors.New("operations outside transactions are not allowed")
	ErrTransactionAlreadyActive    = errors.New("transaction already active")
	ErrUnsupportedCommand          = errors.New("unsupported command")
)

func NewSession(txManager *tx.Manager, kvStore *kvstore.KVStore, txID *uint64) (*Session, error) {
	session := &Session{
		txManager:   txManager,
		kvStore:     kvStore,
		currentTxID: txID,
	}

	if txID == nil {
		return session, nil
	}

	activeTx, err := txManager.GetActiveTx(tx.ID(*txID))
	if err != nil {
		return nil, err
	}

	session.currentTx = activeTx
	return session, nil
}

func (s *Session) ExecuteBulk(commands []*command.Command) []command.ExecutionResult {
	results := make([]command.ExecutionResult, len(commands))
	for i, cmd := range commands {
		results[i] = s.Execute(cmd)
		if results[i].Err != nil {
			break
		}
	}

	return results
}

func (s *Session) Execute(cmd *command.Command) command.ExecutionResult {
	definition := command.DefinitionByKeyword(cmd.Keyword)
	if definition == nil || definition.Handler == nil {
		return command.ExecutionResult{TxID: s.currentTxID, Err: ErrUnsupportedCommand}
	}

	result := definition.Handler(s, s, cmd)
	result.TxID = s.currentTxID

	if result.Err != nil {
		s.Abort()
	}

	return result
}

func (s *Session) Abort() {
	if !s.hasCurrentTx() {
		return
	}

	s.currentTx.Abort()
	s.currentTx = nil
	s.currentTxID = nil
}

func (s *Session) hasCurrentTx() bool {
	return s.currentTx != nil
}
