package main

import "kv/tx"

type Store interface {
	Get(key string, transaction *tx.Transaction) ([]byte, error)
	Set(key string, value []byte, transaction *tx.Transaction) error
	Delete(key string, transaction *tx.Transaction) error
}
