package executor

import "kv/internal/command"

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

func handleBegin(tx TxControl, _ Store, _ *command.Command) ExecutionResult {
	err := tx.Begin()
	return ExecutionResult{Err: err}
}

func handleCommit(tx TxControl, _ Store, _ *command.Command) ExecutionResult {
	err := tx.Commit()
	return ExecutionResult{Err: err}
}

func handleAbort(tx TxControl, _ Store, _ *command.Command) ExecutionResult {
	err := tx.AbortTx()
	return ExecutionResult{Err: err}
}
