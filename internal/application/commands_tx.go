package application

import (
	"kv/internal/command"
)

func RegisterTxCommands() {
	RegisterHandler("TXBEGIN", handleBegin)
	command.RegisterDefinition(&command.Definition{
		Keyword:   "TXBEGIN",
		Arguments: nil,
		Meta: command.Meta{
			Name:        "TXBEGIN",
			Usage:       "TXBEGIN",
			Description: "Start a new transaction",
		},
	})

	RegisterHandler("TXCOMMIT", handleCommit)
	command.RegisterDefinition(&command.Definition{
		Keyword:   "TXCOMMIT",
		Arguments: nil,
		Meta: command.Meta{
			Name:        "TXCOMMIT",
			Usage:       "TXCOMMIT",
			Description: "Commit current transaction",
		},
	})

	RegisterHandler("TXABORT", handleAbort)
	command.RegisterDefinition(&command.Definition{
		Keyword:   "TXABORT",
		Arguments: nil,
		Meta: command.Meta{
			Name:        "TXABORT",
			Usage:       "TXABORT",
			Description: "Abort current transaction",
		},
	})
}

func handleBegin(session *Session, _ *command.Command) ExecutionResult {
	err := session.Begin()
	return ExecutionResult{Err: err}
}

func handleCommit(session *Session, _ *command.Command) ExecutionResult {
	err := session.Commit()
	return ExecutionResult{Err: err}
}

func handleAbort(session *Session, _ *command.Command) ExecutionResult {
	err := session.AbortTx()
	return ExecutionResult{Err: err}
}
