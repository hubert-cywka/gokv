package main

import (
	"flag"
)

type StartMode int

const (
	Unknown StartMode = iota
	StartModeRepl
	StartModeHTTP
)

func parseStartMode(args []string) (StartMode, error) {
	flags := flag.NewFlagSet("kv", flag.ContinueOnError)
	repl := flags.Bool("repl", false, "start repl")
	http := flags.Bool("http", false, "start http api server")

	if err := flags.Parse(args); err != nil {
		return 0, err
	}

	if *repl {
		return StartModeRepl, nil
	}

	if *http {
		return StartModeHTTP, nil
	}

	// TODO: Return error if no mode was selected
	// TODO: Return error if more than one mode was selected

	return 0, nil
}
