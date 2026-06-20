package main

import "github.com/rs/zerolog"

func setLoggingLevel(level zerolog.Level) {
	zerolog.SetGlobalLevel(level)
}
