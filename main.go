package main

import (
	"fmt"
	"kv/engine"
	"kv/engine/mvcc"
	"kv/engine/tx"
	"kv/engine/wal"
	"kv/kvstore"
	"kv/observability"
	"kv/storage"
	"os"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// TODO: Run autovacuum

func main() {
	observability.SetLoggingLevel(zerolog.InfoLevel)

	if err := run(DefaultConfig()); err != nil {
		log.Fatal().Err(err).Msg("application startup failed")
	}
}

func run(cfg Config) (err error) {
	storageManager := storage.NewManager()

	var closers Disposer
	defer func() {
		if err = closers.Dispose(); err != nil {
			return
		}
	}()

	writeAheadLog, err := bootstrapWriteAheadLog(storageManager, cfg, &closers)
	if err != nil {
		return err
	}

	txManager, err := bootstrapTxManager(storageManager, writeAheadLog, cfg, &closers)
	if err != nil {
		return err
	}

	kvStore, err := bootstrapKVStore(writeAheadLog, writeAheadLog, cfg)
	if err != nil {
		return err
	}

	return startRepl(txManager, kvStore)
}

func bootstrapWriteAheadLog(storageManager *storage.Manager, cfg Config, closers *Disposer) (*wal.WriteAheadLog, error) {
	logManifestFile, err := storageManager.Open(cfg.LogManifestPath, os.O_RDWR|os.O_CREATE)
	if err != nil {
		return nil, fmt.Errorf("failed to open log manifest: %w", err)
	}
	closers.Track(logManifestFile)

	logManifest := wal.NewManifest(logManifestFile)

	logOptions := wal.LogOptions{
		LogsDirectory: cfg.LogDir,
		SegmentSize:   cfg.LogSegmentSize,
	}

	logStream, err := wal.NewLog(logManifest, logOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to create log stream: %w", err)
	}
	closers.Track(logStream)

	writeAheadLog := wal.NewWriteAheadLog(wal.Options{
		WriterBufferSize:    cfg.WalBufferSize,
		BatchCommitWaitTime: cfg.WalCommitWait,
	}, logStream)

	closers.Track(writeAheadLog)

	return writeAheadLog, nil
}

func bootstrapTxManager(storageManager *storage.Manager, walAppender wal.Appender, cfg Config, closers *Disposer) (*tx.Manager, error) {
	tmManifestFile, err := storageManager.Open(cfg.TxManifestPath, os.O_RDWR|os.O_CREATE)
	if err != nil {
		return nil, fmt.Errorf("failed to open tx manifest: %w", err)
	}
	closers.Track(tmManifestFile)

	txManifest := tx.NewManifest(tmManifestFile)

	manager := tx.NewManager(txManifest, walAppender, tx.ManagerOptions{
		ReservedIDsPerBatch:   cfg.ReservedTxIDsPerBatch,
		MaxActiveTransactions: cfg.MaxActiveTx,
	})

	return manager, nil
}

func bootstrapKVStore(walReplayer wal.Replayer, walAppender wal.Appender, cfg Config) (*kvstore.KVStore, error) {
	versionMap := mvcc.NewVersionMap()
	mvccStore := mvcc.NewStore(versionMap)
	recoveryManager := engine.NewRecoveryManager(versionMap, walReplayer)

	if err := recoveryManager.Run(); err != nil {
		return nil, fmt.Errorf("recovery failed: %w", err)
	}

	storageEngine := engine.New(mvccStore, walAppender)

	kvOptions := kvstore.Options{
		Validation: kvstore.ValidationOptions{
			MaxKeySize:   cfg.MaxKeySize,
			MaxValueSize: cfg.MaxValueSize,
		},
	}

	return kvstore.New(storageEngine, kvOptions), nil
}
