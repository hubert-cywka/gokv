package main

import (
	"fmt"
	"kv/engine/tx"
	"kv/engine/wal"
	"os"
)

func bootstrapTxManager(walAppender wal.Appender, cfg Config, closers *Disposer) (*tx.Manager, error) {
	tmManifestFile, err := openFile(cfg.TxManifestPath, os.O_RDWR|os.O_CREATE)
	if err != nil {
		return nil, fmt.Errorf("failed to open tx manifest: %w", err)
	}
	closers.Track(tmManifestFile)

	txManifest := tx.NewManifest(tmManifestFile)

	manager := tx.NewManager(txManifest, walAppender, tx.ManagerOptions{
		ReservedIDsPerBatch:   cfg.ReservedTxIDsPerBatch,
		MaxActiveTransactions: cfg.MaxActiveTx,
		Timeout:               cfg.TxTimeout,
	})

	return manager, nil
}
