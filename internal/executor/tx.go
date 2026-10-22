package executor

import "kv/internal/command"

func RegisterTxCommands() {
	command.RegisterDefinition(&command.Definition{
		Keyword:   "TXBEGIN",
		Arguments: nil,
		Handler:   handleBegin,
		Meta: command.Meta{
			Name:        "TXBEGIN",
			Usage:       "TXBEGIN",
			Description: "Start a new transaction",
		},
	})

	command.RegisterDefinition(&command.Definition{
		Keyword:   "TXCOMMIT",
		Arguments: nil,
		Handler:   handleCommit,
		Meta: command.Meta{
			Name:        "TXCOMMIT",
			Usage:       "TXCOMMIT",
			Description: "Commit current transaction",
		},
	})

	command.RegisterDefinition(&command.Definition{
		Keyword:   "TXABORT",
		Arguments: nil,
		Handler:   handleAbort,
		Meta: command.Meta{
			Name:        "TXABORT",
			Usage:       "TXABORT",
			Description: "Abort current transaction",
		},
	})
}

func handleBegin(tx command.TxControl, _ command.Store, _ *command.Command) command.ExecutionResult {
	err := tx.Begin()
	return command.ExecutionResult{Err: err}
}

func handleCommit(tx command.TxControl, _ command.Store, _ *command.Command) command.ExecutionResult {
	err := tx.Commit()
	return command.ExecutionResult{Err: err}
}

func handleAbort(tx command.TxControl, _ command.Store, _ *command.Command) command.ExecutionResult {
	err := tx.AbortTx()
	return command.ExecutionResult{Err: err}
}
