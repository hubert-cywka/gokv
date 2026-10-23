package server

import (
	"fmt"
	"kv/internal/config"
	"kv/internal/engine/mvcc"
	"kv/internal/engine/record"
	"kv/internal/engine/recovery"
	"kv/internal/engine/tx"
	"kv/internal/engine/wal"
	"kv/internal/kvstore"
	"os"
	"path/filepath"
)

func newKVStore(versionMap *mvcc.VersionMap, replayer recovery.Replayer, appender record.Appender, cfg config.Config) (*kvstore.KVStore, error) {
	mvccStore := mvcc.NewStore(versionMap)
	recoveryManager := recovery.NewManager(versionMap, replayer)

	if err := recoveryManager.Recover(); err != nil {
		return nil, fmt.Errorf("recovery failed: %w", err)
	}

	kvOptions := kvstore.Options{
		Validation: kvstore.ValidationOptions{
			MaxKeySize:   cfg.MaxKeySize,
			MaxValueSize: cfg.MaxValueSize,
		},
	}

	return kvstore.New(mvccStore, appender, kvOptions), nil
}

func newTxManager(walAppender record.Appender, cfg config.Config, closers *Disposer) (*tx.Manager, error) {
	tmManifestFile, err := openFile(cfg.TxManifestPath, os.O_RDWR|os.O_CREATE)
	if err != nil {
		return nil, fmt.Errorf("failed to open tx manifest: %w", err)
	} else {
		closers.Track(tmManifestFile)
	}

	txManifest := tx.NewManifest(tmManifestFile)

	manager := tx.NewManager(txManifest, walAppender, tx.ManagerOptions{
		ReservedIDsPerBatch:   cfg.ReservedTxIDsPerBatch,
		MaxActiveTransactions: cfg.MaxActiveTx,
		Timeout:               cfg.TxTimeout,
	})

	return manager, nil
}

func newWriteAheadLog(cfg config.Config, closers *Disposer) (*wal.WriteAheadLog, error) {
	logManifestFile, err := openFile(cfg.LogManifestPath, os.O_RDWR|os.O_CREATE)
	if err != nil {
		return nil, fmt.Errorf("failed to open log manifest: %w", err)
	} else {
		closers.Track(logManifestFile)
	}

	logManifest := wal.NewManifest(logManifestFile)

	logOptions := wal.LogOptions{
		LogsDirectory: cfg.LogDir,
		SegmentSize:   cfg.LogSegmentSize,
	}

	logStream, err := wal.NewLog(logManifest, logOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to create log stream: %w", err)
	} else {
		closers.Track(logStream)
	}

	writeAheadLog := wal.NewWriteAheadLog(wal.Options{
		WriterBufferSize:    cfg.WalBufferSize,
		BatchCommitWaitTime: cfg.WalCommitWait,
	}, logStream)

	closers.Track(writeAheadLog)

	return writeAheadLog, nil
}

func openFile(filename string, flag int) (*os.File, error) {
	dir := filepath.Dir(filename)

	if err := ensureDirectoryExists(dir); err != nil {
		return nil, err
	}

	file, err := os.OpenFile(filename, flag, 0644)
	if err != nil {
		return nil, err
	}

	return file, nil
}

func ensureDirectoryExists(directory string) error {
	return os.MkdirAll(directory, 0755)
}
