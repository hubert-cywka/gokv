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
	options   ExecutionOptions

	currentTx   *tx.Transaction
	currentTxID *uint64
}

var (
	ErrNoActiveTransaction      = errors.New("no active transaction")
	ErrTransactionAlreadyActive = errors.New("transaction already active")
	ErrUnsupportedCommand       = errors.New("unsupported command")
)

type ExecutionOptions struct {
	AbortTransactionOnError bool
}

func NewSession(txManager *tx.Manager, kvStore *kvstore.KVStore, txID *uint64, options ExecutionOptions) (*Session, error) {
	session := &Session{
		txManager:   txManager,
		kvStore:     kvStore,
		options:     options,
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

func (s *Session) Execute(cmd *command.Command) command.ExecutionResult {
	definition := command.DefinitionByKeyword(cmd.Keyword)
	if definition == nil || definition.Handler == nil {
		return command.ExecutionResult{TxID: s.currentTxID, Err: ErrUnsupportedCommand}
	}

	result := definition.Handler(s, s, cmd)
	if result.TxID == nil {
		result.TxID = s.currentTxID
	}

	if result.Err != nil && s.options.AbortTransactionOnError {
		s.Abort()
		result.TxID = s.currentTxID
	}

	return result
}

func (s *Session) Abort() {
	if s.currentTx == nil {
		return
	}

	s.currentTx.Abort()
	s.currentTx = nil
}
