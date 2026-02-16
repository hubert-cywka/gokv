package query

import (
	"errors"
	"strings"
)

// TODO: Parser, lexer, custom (simple) query language

var InvalidCommandError = errors.New("invalid command")
var InvalidKeyError = errors.New("invalid key")
var InvalidNumberOfTokens = errors.New("invalid number of tokens")

func Parse(input string) (*Command, error) {
	trimmedInput := strings.TrimSpace(input)
	if trimmedInput == "" {
		return nil, InvalidCommandError
	}

	tokens, err := tokenize(trimmedInput)

	if err != nil {
		return nil, err
	}

	if len(tokens) == 0 {
		return nil, InvalidCommandError
	}

	switch strings.ToUpper(tokens[0]) {
	case SET:
		if len(tokens) != 3 {
			return nil, InvalidNumberOfTokens
		}

		key := tokens[1]
		value := tokens[2]

		if !isValidKey(key) {
			return nil, InvalidKeyError
		}

		return &Command{
			Key:   key,
			Value: []byte(value),
			Type:  CommandSet,
		}, nil

	case GET:
		if len(tokens) != 2 {
			return nil, InvalidNumberOfTokens
		}

		key := tokens[1]

		if !isValidKey(key) {
			return nil, InvalidKeyError
		}

		return &Command{
			Key:  key,
			Type: CommandGet,
		}, nil

	case DELETE:
		if len(tokens) != 2 {
			return nil, InvalidNumberOfTokens
		}

		key := tokens[1]

		if !isValidKey(key) {
			return nil, InvalidKeyError
		}

		return &Command{
			Key:  key,
			Type: CommandDelete,
		}, nil

	case EXIT:
		if len(tokens) != 1 {
			return nil, InvalidNumberOfTokens
		}

		return &Command{
			Type: CommandExit,
		}, nil

	case HELP:
		if len(tokens) != 1 {
			return nil, InvalidNumberOfTokens
		}

		return &Command{
			Type: CommandHelp,
		}, nil

	case TRANSACTION:
		if len(tokens) != 2 {
			return nil, InvalidNumberOfTokens
		}

		switch strings.ToUpper(tokens[1]) {
		case BEGIN:
			return &Command{
				Type: CommandBegin,
			}, nil

		case ABORT:
			return &Command{
				Type: CommandAbort,
			}, nil

		case COMMIT:
			return &Command{
				Type: CommandCommit,
			}, nil

		default:
			return nil, InvalidCommandError
		}

	default:
		return nil, InvalidCommandError
	}
}

func isValidKey(key string) bool {
	for i := 0; i < len(key); i++ {
		c := key[i]

		switch {
		case c >= 'a' && c <= 'z':
		case c >= 'A' && c <= 'Z':
		case c >= '0' && c <= '9':
		case c == '_', c == '-', c == '.':
		default:
			return false
		}
	}

	return true
}
