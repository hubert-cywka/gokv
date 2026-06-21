package main

import (
	"fmt"
	"kv/engine"
	"kv/engine/mvcc"
	"kv/engine/wal"
	"kv/kvstore"
)

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
