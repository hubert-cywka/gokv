package main

import "time"

type Config struct {
	VacuumInterval time.Duration

	TxManifestPath        string
	ReservedTxIDsPerBatch uint64
	MaxActiveTx           uint16
	TxTimeout             time.Duration

	MaxKeySize   int
	MaxValueSize int

	WalBufferSize   int
	WalCommitWait   time.Duration
	LogSegmentSize  int64
	LogDir          string
	LogManifestPath string
}

func DefaultConfig() Config {
	return Config{
		VacuumInterval: 120 * time.Second,

		TxManifestPath:        "./data/transactions/manifest.json",
		ReservedTxIDsPerBatch: 1000,
		MaxActiveTx:           100,
		TxTimeout:             60 * time.Second,

		MaxKeySize:   1024,
		MaxValueSize: 128 * 1024,

		WalBufferSize:   512 * 1024,
		WalCommitWait:   5 * time.Millisecond,
		LogSegmentSize:  512 * 1024,
		LogDir:          "./data/log",
		LogManifestPath: "./data/log/manifest.json",
	}
}
