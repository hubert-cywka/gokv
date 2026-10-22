package server

import (
	"context"
	"fmt"
	"kv/internal/api/http_server"
	"kv/internal/api/repl_server"
	"kv/internal/config"
	"kv/internal/engine"
	"kv/internal/engine/mvcc"
	"kv/internal/engine/tx"
	"kv/internal/engine/wal"
	"kv/internal/kvstore"
	"os"
	"os/signal"
)

func Run(cfg config.Config, mode StartMode, ctx context.Context) (err error) {
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
	defer cancel()

	var closers Disposer
	defer func() {
		if err = closers.Dispose(); err != nil {
			return
		}
	}()

	writeAheadLog, err := newWriteAheadLog(cfg, &closers)
	if err != nil {
		return err
	}

	txManager, err := newTxManager(writeAheadLog, cfg, &closers)
	if err != nil {
		return err
	}

	versionMap := mvcc.NewVersionMap()
	kvStore, err := newKVStore(versionMap, writeAheadLog, writeAheadLog, cfg)
	if err != nil {
		return err
	}

	// TODO: "Smart" autovacuum - do not run it if there is no need to.
	vacuumer := engine.NewVacuumer(versionMap, writeAheadLog)
	vacuumer.RunOnInterval(txManager, cfg.VacuumInterval, ctx)

	if mode == StartModeRepl {
		return repl_server.Start(txManager, kvStore, ctx)
	}

	if mode == StartModeHTTP {
		return http_server.Start(cfg.HTTPAddress, txManager, kvStore, ctx)
	}

	return nil
}

func newKVStore(versionMap *mvcc.VersionMap, walReplayer wal.Replayer, walAppender wal.Appender, cfg config.Config) (*kvstore.KVStore, error) {
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

func newTxManager(walAppender wal.Appender, cfg config.Config, closers *Disposer) (*tx.Manager, error) {
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

func newWriteAheadLog(cfg config.Config, closers *Disposer) (*wal.WriteAheadLog, error) {
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
