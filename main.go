package main

import (
	"context"
	"fmt"
	"kv/engine"
	"kv/engine/mvcc"
	"kv/engine/tx"
	"kv/engine/wal"
	"kv/kvstore"
	"kv/observability"
	"kv/storage"
	"os"
	"path/filepath"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	observability.SetLoggingLevel(zerolog.InfoLevel)

	if err := run(DefaultConfig()); err != nil {
		log.Fatal().Err(err).Msg("application startup failed")
	}
}

func run(cfg Config) (err error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var closers Disposer
	defer func() {
		if err = closers.Dispose(); err != nil {
			return
		}
	}()

	writeAheadLog, err := bootstrapWriteAheadLog(cfg, &closers)
	if err != nil {
		return err
	}

	txManager, err := bootstrapTxManager(writeAheadLog, cfg, &closers)
	if err != nil {
		return err
	}

	versionMap := mvcc.NewVersionMap()
	kvStore, err := bootstrapKVStore(versionMap, writeAheadLog, writeAheadLog, cfg)
	if err != nil {
		return err
	}

	// TODO: "Smart" autovacuum - do not run it if there is no need to.
	vacuumer := engine.NewVacuumer(versionMap, writeAheadLog)
	vacuumer.RunOnInterval(txManager, cfg.VacuumInterval, ctx)

	return startRepl(txManager, kvStore)
}

func bootstrapWriteAheadLog(cfg Config, closers *Disposer) (*wal.WriteAheadLog, error) {
	logManifestFile, err := openFile(cfg.LogManifestPath, os.O_RDWR|os.O_CREATE)
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
		TimeoutMs:             cfg.TxTimeoutMs,
	})

	return manager, nil
}

func bootstrapKVStore(
	versionMap *mvcc.VersionMap,
	walReplayer wal.Replayer,
	walAppender wal.Appender,
	cfg Config,
) (*kvstore.KVStore, error) {
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

func openFile(filename string, flag int) (*os.File, error) {
	dir := filepath.Dir(filename)

	if err := storage.EnsureDirectoryExists(dir); err != nil {
		return nil, err
	}

	file, err := os.OpenFile(filename, flag, 0644)

	if err != nil {
		return nil, err
	}

	return file, nil
}
