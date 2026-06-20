package main

import "kv/command"

func RegisterCoreCommandDefinitions() {
	command.RegisterDefinition(&command.Definition{
		Keyword: "SET",
		Arguments: []command.ArgumentDefinition{
			{Validator: command.KeyValidator{}},
			{Validator: command.AnyValidator{}},
		},
		Handler: handleSet,
		Meta: command.Meta{
			Name:        "SET",
			Usage:       "SET <key> <value>",
			Description: "Set value for a key",
		},
	})

	command.RegisterDefinition(&command.Definition{
		Keyword: "GET",
		Arguments: []command.ArgumentDefinition{
			{Validator: command.KeyValidator{}},
		},
		Handler: handleGet,
		Meta: command.Meta{
			Name:        "GET",
			Usage:       "GET <key>",
			Description: "Get value of a key",
		},
	})

	command.RegisterDefinition(&command.Definition{
		Keyword: "DELETE",
		Arguments: []command.ArgumentDefinition{
			{Validator: command.KeyValidator{}},
		},
		Handler: handleDelete,
		Meta: command.Meta{
			Name:        "DELETE",
			Usage:       "DELETE <key>",
			Description: "Delete a key",
		},
	})

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
	return command.ExecutionResult{TxID: tx.CurrentTxID(), Err: err}
}

func handleCommit(tx command.TxControl, _ command.Store, _ *command.Command) command.ExecutionResult {
	err := tx.Commit()
	return command.ExecutionResult{TxID: tx.CurrentTxID(), Err: err}
}

func handleAbort(tx command.TxControl, _ command.Store, _ *command.Command) command.ExecutionResult {
	err := tx.AbortTx()
	return command.ExecutionResult{TxID: tx.CurrentTxID(), Err: err}
}

func handleSet(tx command.TxControl, store command.Store, cmd *command.Command) command.ExecutionResult {
	err := store.Set(string(cmd.Arguments[0]), cmd.Arguments[1])
	return command.ExecutionResult{TxID: tx.CurrentTxID(), Err: err}
}

func handleGet(tx command.TxControl, store command.Store, cmd *command.Command) command.ExecutionResult {
	value, err := store.Get(string(cmd.Arguments[0]))
	return command.ExecutionResult{TxID: tx.CurrentTxID(), Value: value, Err: err}
}

func handleDelete(tx command.TxControl, store command.Store, cmd *command.Command) command.ExecutionResult {
	err := store.Delete(string(cmd.Arguments[0]))
	return command.ExecutionResult{TxID: tx.CurrentTxID(), Err: err}
}
