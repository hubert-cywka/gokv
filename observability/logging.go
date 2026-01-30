package observability

import "github.com/rs/zerolog"

func SetLoggingLevel(level zerolog.Level) {
	zerolog.SetGlobalLevel(level)
}
