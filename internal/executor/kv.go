package executor

import "kv/internal/command"

func RegisterKVCommands() {
	RegisterHandler("SET", handleSet)
	command.RegisterDefinition(&command.Definition{
		Keyword: "SET",
		Arguments: []command.ArgumentDefinition{
			{Validator: command.KeyValidator{}},
			{Validator: command.AnyValidator{}},
		},
		Meta: command.Meta{
			Name:        "SET",
			Usage:       "SET <key> <value>",
			Description: "Set value for a key",
		},
	})

	RegisterHandler("GET", handleGet)
	command.RegisterDefinition(&command.Definition{
		Keyword: "GET",
		Arguments: []command.ArgumentDefinition{
			{Validator: command.KeyValidator{}},
		},
		Meta: command.Meta{
			Name:        "GET",
			Usage:       "GET <key>",
			Description: "Get value of a key",
		},
	})

	RegisterHandler("DELETE", handleDelete)
	command.RegisterDefinition(&command.Definition{
		Keyword: "DELETE",
		Arguments: []command.ArgumentDefinition{
			{Validator: command.KeyValidator{}},
		},
		Meta: command.Meta{
			Name:        "DELETE",
			Usage:       "DELETE <key>",
			Description: "Delete a key",
		},
	})
}

func handleSet(tx TxControl, store Store, cmd *command.Command) ExecutionResult {
	err := store.Set(string(cmd.Arguments[0]), cmd.Arguments[1])
	return ExecutionResult{Err: err}
}

func handleGet(tx TxControl, store Store, cmd *command.Command) ExecutionResult {
	value, err := store.Get(string(cmd.Arguments[0]))
	return ExecutionResult{Value: value, Err: err}
}

func handleDelete(tx TxControl, store Store, cmd *command.Command) ExecutionResult {
	err := store.Delete(string(cmd.Arguments[0]))
	return ExecutionResult{Err: err}
}
