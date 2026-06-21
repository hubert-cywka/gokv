package http_server

import (
	"context"
	"errors"
	"kv/engine/tx"
	"kv/kvstore"
	"net/http"
	"time"

	"github.com/rs/zerolog/log"
)

// TODO: Auth
// TODO: Clean up
func Start(address string, txManager *tx.Manager, kvStore *kvstore.KVStore, ctx context.Context) error {
	server := new(address, txManager, kvStore, ctx)

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

func new(address string, txManager *tx.Manager, kvStore *kvstore.KVStore, ctx context.Context) *http.Server {
	mux := http.NewServeMux()

	mux.Handle("/query", createQueryHandler(txManager, kvStore, ctx))

	return &http.Server{
		Addr:    address,
		Handler: mux,
	}
}
