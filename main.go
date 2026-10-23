package main

import (
	"context"
	"kv/internal/application"
	"kv/internal/config"
	"kv/internal/server"
	"os"
	"os/signal"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	cfg := config.Default()

	mode, err := server.ParseStartArguments(os.Args[1:])
	if err != nil {
		log.Fatal().Err(err).Msg("invalid start arguments")
	}

	server, err := server.New(cfg, mode)
	if err != nil {
		log.Fatal().Err(err).Msg("application startup failed")
	}

	defer func() {
		if err := server.Close(); err != nil {
			log.Error().Err(err).Msg("application close failed")
		}
	}()

	application.RegisterKVCommands()
	application.RegisterTxCommands()

	if err := server.Run(ctx); err != nil {
		log.Fatal().Err(err).Msg("application startup failed")
	}

	log.Info().Msg("application closed")
}
