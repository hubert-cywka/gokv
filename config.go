package main

import "time"

type Config struct {
	LogDir          string
	LogManifestPath string

	TxManifestPath        string
	ReservedTxIDsPerBatch uint64
	MaxActiveTx           uint16

	MaxKeySize   int
	MaxValueSize int

	WalBufferSize  int
	WalCommitWait  time.Duration
	LogSegmentSize int64
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
	}
}
