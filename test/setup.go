package test

import (
	"kv/otel"

	"github.com/rs/zerolog"
)

func DisableLogging() {
	otel.SetLoggingLevel(zerolog.Disabled)
}
