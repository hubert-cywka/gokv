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
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// TODO: Clean up this mess and set up a proper server instead of a demo

type Config struct {
	LogDir                string
	LogManifestPath       string
	TxManifestPath        string
	ReservedTxIDsPerBatch uint64
	MaxActiveTx           uint16
	MaxKeySize            int
	MaxValueSize          int
	WalBufferSize         int
	WalCommitWait         time.Duration
	LogSegmentSize        int64
	WorkerCount           int
}

func DefaultConfig() Config {
	return Config{
		LogDir:                "./internals/log",
		LogManifestPath:       "./internals/log/manifest.json",
		TxManifestPath:        "./internals/transactions/manifest.json",
		ReservedTxIDsPerBatch: 1000,
		MaxActiveTx:           100,
		MaxKeySize:            1024,
		MaxValueSize:          128 * 1024,
		WalBufferSize:         512 * 1024,
		WalCommitWait:         5 * time.Millisecond,
		LogSegmentSize:        512 * 1024,
		WorkerCount:           100,
	}
}

func main() {
	observability.SetLoggingLevel(zerolog.InfoLevel)

	if err := run(DefaultConfig()); err != nil {
		log.Fatal().Err(err).Msg("application startup failed")
	}
}

func run(cfg Config) error {
	fm := storage.NewManager()

	logManifestFile, err := fm.Open(cfg.LogManifestPath, os.O_RDWR|os.O_CREATE)
	if err != nil {
		return fmt.Errorf("failed to open log manifest: %w", err)
	}
	defer logManifestFile.Close()

	logManifest := wal.NewManifest(logManifestFile)
	logStream, err := wal.NewLog(logManifest, wal.LogOptions{
		LogsDirectory: cfg.LogDir,
		SegmentSize:   cfg.LogSegmentSize,
	})
	if err != nil {
		return fmt.Errorf("failed to create log stream: %w", err)
	}

	writeAheadLog, err := wal.NewWriteAheadLog(wal.Options{
		WriterBufferSize:    cfg.WalBufferSize,
		BatchCommitWaitTime: cfg.WalCommitWait,
	}, logStream)
	if err != nil {
		return fmt.Errorf("failed to initialize WAL: %w", err)
	}
	defer func() {
		if err := writeAheadLog.Close(); err != nil {
			log.Error().Err(err).Msg("failed to close WAL cleanly")
		}
	}()

	tmManifestFile, err := fm.Open(cfg.TxManifestPath, os.O_RDWR|os.O_CREATE)
	if err != nil {
		return fmt.Errorf("failed to open tx manifest: %w", err)
	}
	defer tmManifestFile.Close()

	txManifest := tx.NewManifest(tmManifestFile)
	txManager := tx.NewManager(txManifest, writeAheadLog, tx.ManagerOptions{
		ReservedIDsPerBatch:   cfg.ReservedTxIDsPerBatch,
		MaxActiveTransactions: cfg.MaxActiveTx,
	})

	versionMap := mvcc.NewVersionMap()
	mvccStore := mvcc.NewStore(versionMap)
	recoveryManager := engine.NewRecoveryManager(versionMap, writeAheadLog)

	if err = recoveryManager.Run(); err != nil {
		return err
	}

	kvOptions := kvstore.Options{
		Validation: kvstore.ValidationOptions{
			MaxKeySize:   cfg.MaxKeySize,
			MaxValueSize: cfg.MaxValueSize,
		},
	}

	storageEngine := engine.New(mvccStore, writeAheadLog)
	kvStore := kvstore.New(storageEngine, kvOptions)

	transaction, err := txManager.Begin()

	if err != nil {
		return err
	}

	fmt.Printf("TxID is %d\n", transaction.ID)
	prev, _ := kvStore.Get("Test", transaction)
	fmt.Printf("prev value is %v", prev)
	_ = kvStore.Set("Test", []byte("AAAA"), transaction)
	_, _ = kvStore.Get("Test", transaction)
	_ = transaction.Commit()

	return nil
}
