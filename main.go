package main

import (
	"kv/memstore"
	"kv/otel"
	"kv/wal"
	"kv/wal/record"
	"kv/wal/storage"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

const (
	defaultWalBufferSize     = 512 * 1024
	defaultWalCommitWaitTime = 5 * time.Millisecond

	defaultLogSegmentSize  = 512 * 1024
	defaultLogDirectory    = "./log"
	defaultLogManifestPath = "./log/manifest.json"

	defaultMaxKeySize   = 1024
	defaultMaxValueSize = 128 * 1024
)

func main() {
	otel.SetLoggingLevel(zerolog.InfoLevel)

	store := memstore.New()

	logStreamOptions := storage.LogOptions{
		ManifestPath:  defaultLogManifestPath,
		LogsDirectory: defaultLogDirectory,
		SegmentSize:   defaultLogSegmentSize,
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
	restoreCacheState(store, writeAheadLog)

	kvOptions := KeyValueStoreOptions{
		Validation: ValidationOptions{
			MaxKeySize:   defaultMaxKeySize,
			MaxValueSize: defaultMaxValueSize,
		},
	}

	kvStore := NewKeyValueStore(store, writeAheadLog, kvOptions)

	log.Info().Msg("server: starting writes")
	var wg sync.WaitGroup

	for i := 1; i <= 100; i++ {
		wg.Go(func() {
			worker(i, kvStore)
		})
	}

	wg.Wait()
	log.Info().Msg("server: finished writes")
}

func worker(index int, kvStore *KeyValueStore) {
	//key := fmt.Sprintf("key-1")
	//value := []byte("value")
	//_ = kvStore.Set(key, value)
}

func closeWal(wal *wal.WriteAheadLog) {
	err := wal.Close()

	if err != nil {
		log.Error().
			Err(err).
			Msg("server: failed to close WAL")
	}
}

func restoreCacheState(cache *memstore.MemStore, wal *wal.WriteAheadLog) {
	log.Info().
		Msg("server: replaying WAL.")

	replayCount := 0

	replayFunc := func(r record.Record) {
		//
		//if r.Kind == record.Tombstone {
		//	replayErr = memstore.Delete(string(r.Key), r.TxID)
		//} else {
		//	replayErr = memstore.Set(string(r.Key), r.Value, r.TxID)
		//}
		//
		//if replayErr != nil {
		//	log.Fatal().
		//		Err(replayErr).
		//		Msg("server: failed to replay WAL")
		//}
		//
		//replayCount++
	}

	if err := wal.Replay(replayFunc); err != nil {
		log.Fatal().
			Err(err).
			Msg("server: failed to replay WAL")
	}

	log.Info().
		Msgf("server: replay complete, restored %d records.", replayCount)
}
