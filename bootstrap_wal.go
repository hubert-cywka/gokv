package main

import (
	"fmt"
	"kv/engine/wal"
	"os"
)

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
