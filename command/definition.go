package command

import (
	"sort"
	"strings"
)

type Command struct {
	Keyword   string
	Arguments [][]byte
}

type Definition struct {
	Keyword   string
	Arguments []ArgumentDefinition

	Meta    Meta
	Handler Handler
}

type ArgumentDefinition struct {
	Validator ArgumentValidator
}

type Meta struct {
	Name        string
	Usage       string
	Description string
}

func Definitions() []*Definition {
	definitions := make([]*Definition, 0, len(definitionsByKeyword))

	for _, definition := range definitionsByKeyword {
		definitions = append(definitions, definition)
	}

	sort.Slice(definitions, func(i, j int) bool {
		return definitions[i].Keyword < definitions[j].Keyword
	})

	return definitions
}

func DefinitionByKeyword(keyword string) *Definition {
	return definitionsByKeyword[normalizeKeyword(keyword)]
}

func RegisterDefinition(definition *Definition) {
	normalizedKeyword := normalizeKeyword(definition.Keyword)
	definition.Keyword = normalizedKeyword
	definitionsByKeyword[normalizedKeyword] = definition
}

func ResetDefinitions() {
	definitionsByKeyword = map[string]*Definition{}
}

var definitionsByKeyword = map[string]*Definition{}

func normalizeKeyword(keyword string) string {
	return strings.ToUpper(strings.TrimSpace(keyword))
}
