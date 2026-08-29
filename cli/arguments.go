package cli

import (
	"errors"
	"flag"
	"strings"
)

type StartMode int

const (
	Unknown StartMode = iota
	StartModeRepl
	StartModeHTTP
)

func ParseStartArguments(args []string) (StartMode, error) {
	flags := flag.NewFlagSet("kv", flag.ContinueOnError)
	mode := flags.String("mode", "http", "kv mode - 'repl' or 'http'")

	if err := flags.Parse(args); err != nil {
		return 0, err
	}

	switch strings.ToLower(*mode) {
	case "repl":
		return StartModeRepl, nil
	case "http":
		return StartModeHTTP, nil
	default:
		return 0, errors.New("invalid start mode")
	}
}
