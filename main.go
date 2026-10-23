package main

import (
	"context"
	"kv/internal/application"
	"kv/internal/config"
	"kv/internal/server"
	"os"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	ctx := context.Background()
	cfg := config.Default()

	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	mode, err := server.ParseStartArguments(os.Args[1:])
	if err != nil {
		log.Fatal().Err(err).Msg("invalid start arguments")
	}

	application.RegisterKVCommands()
	application.RegisterTxCommands()

	if err := server.Run(cfg, mode, ctx); err != nil {
		log.Fatal().Err(err).Msg("application startup failed")
	}

	log.Info().Msg("application closed")
}
