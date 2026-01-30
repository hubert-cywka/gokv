package main

import (
	"kv/cache"
	"kv/observability"
	"kv/storage"
	"kv/wal"
	"kv/wal/data"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

const (
	defaultWalBufferSize     = 512 * 1024
	defaultWalCommitWaitTime = 5 * time.Minute

	defaultLogSegmentSize = 512 * 1024
	defaultLogDirectory   = "./log"

	defaultMaxKeySize   = 1024
	defaultMaxValueSize = 128 * 1024

	defaultCachePartitions               = 64
	defaultCachePartitionInitialCapacity = 16 * 1024
)

func main() {
	observability.SetLoggingLevel(zerolog.InfoLevel)

	cacheOptions := cache.Options{
		Partitions:      defaultCachePartitions,
		InitialCapacity: defaultCachePartitionInitialCapacity,
	}
	partitionedCache := cache.NewPartitionedCache(cacheOptions)

	logStreamOptions := storage.LogOptions{
		SegmentSize:   defaultLogSegmentSize,
		LogsDirectory: defaultLogDirectory,
	}
	logStream, _ := storage.NewLog(logStreamOptions)

	walOptions := wal.WriteAheadLogOptions{
		WriterBufferSize:    defaultWalBufferSize,
		BatchCommitWaitTime: defaultWalCommitWaitTime,
	}
	writeAheadLog, err := wal.NewWriteAheadLog(walOptions, logStream)

	if err != nil {
		log.Fatal().
			Err(err).
			Msg("server: failed to initialize WAL")
	}

	defer closeWal(writeAheadLog)
	restoreCacheState(partitionedCache, writeAheadLog)

	kvOptions := KeyValueStoreOptions{
		Validation: ValidationOptions{
			MaxKeySize:   defaultMaxKeySize,
			MaxValueSize: defaultMaxValueSize,
		},
	}

	kvStore := NewKeyValueStore(partitionedCache, writeAheadLog, kvOptions)
	_, _ = kvStore.Get("empty")
}

func closeWal(wal *wal.WriteAheadLog) {
	err := wal.Close()

	if err != nil {
		log.Error().
			Err(err).
			Msg("server: failed to close WAL")
	}
}

func restoreCacheState(cache *cache.PartitionedCache, wal *wal.WriteAheadLog) {
	log.Info().
		Msg("server: replaying WAL.")

	replayCount := 0
	var replayErr error

	replayFunc := func(record data.Record) {

		if record.Kind() == data.Delete {
			replayErr = cache.Delete(string(record.Key()))
		} else {
			replayErr = cache.Set(string(record.Key()), record.Value())
		}

		if replayErr != nil {
			log.Fatal().
				Err(replayErr).
				Msg("server: failed to replay WAL")
		}

		replayCount++
	}

	if err := wal.Replay(replayFunc); err != nil {
		log.Fatal().
			Err(err).
			Msg("server: failed to replay WAL")
	}

	log.Info().
		Msgf("server: replay complete, restored %d records.", replayCount)
}
