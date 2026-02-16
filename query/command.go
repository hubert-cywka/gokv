package query

type CommandType uint8

const (
	CommandSet CommandType = iota
	CommandGet
	CommandDelete

	CommandBegin
	CommandCommit
	CommandAbort

	CommandExit
	CommandHelp
)

type Command struct {
	Type  CommandType
	Key   string
	Value []byte
}

type CommandMeta struct {
	Name        string
	Usage       string
	Description string
}

var CommandRegistry = map[CommandType]CommandMeta{
	CommandBegin: {
		Name:        "TRANSACTION BEGIN",
		Usage:       "TRANSACTION BEGIN",
		Description: "Start a new transaction",
	},
	CommandCommit: {
		Name:        "TRANSACTION COMMIT",
		Usage:       "TRANSACTION COMMIT",
		Description: "Commit current transaction",
	},
	CommandAbort: {
		Name:        "TRANSACTION ABORT",
		Usage:       "TRANSACTION ABORT",
		Description: "Abort current transaction",
	},
	CommandGet: {
		Name:        "GET",
		Usage:       "GET <key>",
		Description: "Get value of a key",
	},
	CommandSet: {
		Name:        "SET",
		Usage:       "SET <key> <value>",
		Description: "Set value for a key",
	},
	CommandDelete: {
		Name:        "DELETE",
		Usage:       "DELETE <key>",
		Description: "Delete a key",
	},
	CommandHelp: {
		Name:        "HELP",
		Usage:       "HELP",
		Description: "Show help message",
	},
	CommandExit: {
		Name:        "EXIT",
		Usage:       "EXIT",
		Description: "Exit the REPL",
	},
}
