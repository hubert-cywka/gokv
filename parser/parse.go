package parser

import (
	"errors"
	"kv/command"
	"strings"
)

var InvalidCommandError = errors.New("invalid command")
var InvalidNumberOfTokens = errors.New("invalid number of tokens")
var InvalidArgument = errors.New("invalid argument")

func ParseBulk(inputs []string) ([]*command.Command, error) {
	commands := make([]*command.Command, len(inputs))
	for i, input := range inputs {
		cmd, parseErr := Parse(input)
		if parseErr != nil {
			return nil, parseErr
		}
		commands[i] = cmd
	}

	return commands, nil
}

func Parse(input string) (*command.Command, error) {
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

	keyword := tokens[0]
	definition := command.DefinitionByKeyword(keyword)
	if definition == nil {
		return nil, InvalidCommandError
	}

	argumentTokens := tokens[1:]
	if len(argumentTokens) != len(definition.Arguments) {
		return nil, InvalidNumberOfTokens
	}

	for i, arg := range argumentTokens {
		if !definition.Arguments[i].Validator.IsValid([]byte(arg)) {
			return nil, InvalidArgument
		}
	}

	args := make([][]byte, len(argumentTokens))
	for i, arg := range argumentTokens {
		args[i] = []byte(arg)
	}

	return &command.Command{
		Keyword:   definition.Keyword,
		Arguments: args,
	}, nil
}
