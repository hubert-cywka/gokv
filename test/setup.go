package test

import (
	"kv/observability"

	"github.com/rs/zerolog"
)

func DisableLogging() {
	observability.SetLoggingLevel(zerolog.Disabled)
}
