package main

import (
	"context"
	"kv/api/http_server"
	"kv/api/repl_server"
	"kv/engine"
	"kv/engine/mvcc"
	"os"
	"os/signal"
	"path/filepath"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	ctx := context.Background()
	cfg := DefaultConfig()

	setLoggingLevel(zerolog.InfoLevel)

	mode, err := parseStartMode(os.Args[1:])
	if err != nil {
		log.Fatal().Err(err).Msg("invalid startup mode")
	}

	if err := run(cfg, mode, ctx); err != nil {
		log.Fatal().Err(err).Msg("application startup failed")
	}

	log.Info().Msg("application closed")
}

func run(cfg Config, mode StartMode, ctx context.Context) (err error) {
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
	defer cancel()

	RegisterCoreCommandDefinitions()

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

	if mode == StartModeRepl {
		return repl_server.Start(txManager, kvStore, ctx)
	}

	if mode == StartModeHTTP {
		return http_server.Start(cfg.HTTPAddress, txManager, kvStore, ctx)
	}

	return nil
}

func openFile(filename string, flag int) (*os.File, error) {
	dir := filepath.Dir(filename)

	if err := ensureDirectoryExists(dir); err != nil {
		return nil, err
	}

	file, err := os.OpenFile(filename, flag, 0644)

	if err != nil {
		return nil, err
	}

	return file, nil
}

func ensureDirectoryExists(directory string) error {
	return os.MkdirAll(directory, 0755)
}
