package executor

import "kv/internal/command"

func RegisterKVCommands() {
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
}

func handleSet(tx command.TxControl, store command.Store, cmd *command.Command) command.ExecutionResult {
	err := store.Set(string(cmd.Arguments[0]), cmd.Arguments[1])
	return command.ExecutionResult{Err: err}
}

func handleGet(tx command.TxControl, store command.Store, cmd *command.Command) command.ExecutionResult {
	value, err := store.Get(string(cmd.Arguments[0]))
	return command.ExecutionResult{Value: value, Err: err}
}

func handleDelete(tx command.TxControl, store command.Store, cmd *command.Command) command.ExecutionResult {
	err := store.Delete(string(cmd.Arguments[0]))
	return command.ExecutionResult{Err: err}
}
