package main

import (
	"context"
	"errors"
	"kv/api/http_server"
	"kv/api/repl_server"
	"kv/engine"
	"kv/engine/mvcc"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"time"

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

	// TODO: Auth
	// TODO: Clean up
	if mode == StartModeHTTP {
		handler := http_server.NewServer(txManager, kvStore, ctx)
		server := &http.Server{Addr: cfg.HTTPAddress, Handler: handler}

		errCh := make(chan error, 1)
		go func() {
			log.Info().Msg("http server started")
			errCh <- server.ListenAndServe()
		}()

		select {
		case <-ctx.Done():
			shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancelShutdown()
			if err := server.Shutdown(shutdownCtx); err != nil {
				return err
			}
			log.Info().Msg("http server closed")
			return nil
		case err := <-errCh:
			if errors.Is(err, http.ErrServerClosed) {
				return nil
			}
			return err
		}
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
