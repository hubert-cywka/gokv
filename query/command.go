package query

type CommandType uint8

const (
	CommandSet CommandType = iota
	CommandGet
	CommandDelete

	CommandBegin
	CommandCommit
	CommandAbort
)

type Command struct {
	Type  CommandType
	Key   string
	Value []byte
}
