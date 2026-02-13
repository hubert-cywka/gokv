package engine

import (
	"kv/engine/mvcc"
	"kv/engine/tx"
	"kv/engine/wal"
	"kv/engine/wal/record"
)

type Engine struct {
	mvccStore   *mvcc.Store
	walAppender wal.Appender
}

func New(mvccStore *mvcc.Store, walAppender wal.Appender) *Engine {
	return &Engine{
		mvccStore:   mvccStore,
		walAppender: walAppender,
	}
}

func (e *Engine) Get(key string, transaction *tx.Transaction) ([]byte, error) {
	value, err := e.mvccStore.Get(key, transaction)
	return value, err
}

func (e *Engine) Set(key string, value []byte, transaction *tx.Transaction) error {
	if err := e.mvccStore.Set(key, value, transaction); err != nil {
		return err
	}

	valueRecord := record.NewValue(key, value, transaction.ID.Uint64())
	if err := e.walAppender.Append(valueRecord); err != nil {
		return err
	}

	return nil
}

func (e *Engine) Delete(key string, transaction *tx.Transaction) error {
	if err := e.mvccStore.Delete(key, transaction); err != nil {
		return err
	}

	tombstoneRecord := record.NewTombstone(key, transaction.ID.Uint64())
	if err := e.walAppender.Append(tombstoneRecord); err != nil {
		return err
	}

	return nil
}
