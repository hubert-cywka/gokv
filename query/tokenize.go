package query

import (
	"errors"
	"strings"
)

func tokenize(input string) ([]string, error) {
	var tokens []string
	var current strings.Builder

	inQuotes := false

	for i := 0; i < len(input); i++ {
		c := input[i]

		switch c {
		case '\'':
			if inQuotes {
				tokens = append(tokens, current.String())
				current.Reset()
				inQuotes = false
			} else {
				if current.Len() > 0 {
					tokens = append(tokens, current.String())
					current.Reset()
				}
				inQuotes = true
			}

		case ' ', '\t', '\n':
			if inQuotes {
				current.WriteByte(c)
				continue
			}

			if current.Len() > 0 {
				tokens = append(tokens, current.String())
				current.Reset()
			}

		default:
			current.WriteByte(c)
		}
	}

	if inQuotes {
		return nil, errors.New("unterminated quoted string")
	}

	if current.Len() > 0 {
		tokens = append(tokens, current.String())
	}

	return tokens, nil
}
