package main

import (
	"flag"
)

type StartMode int

const (
	Unknown StartMode = iota
	StartModeRepl
)

func parseStartMode(args []string) (StartMode, error) {
	flags := flag.NewFlagSet("kv", flag.ContinueOnError)
	repl := flags.Bool("repl", false, "start repl")

	if err := flags.Parse(args); err != nil {
		return 0, err
	}

	if *repl {
		return StartModeRepl, nil
	}

	return 0, nil
}
