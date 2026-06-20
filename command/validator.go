package command

type ArgumentValidator interface {
	IsValid([]byte) bool
}

type AnyValidator struct{}

func (v AnyValidator) IsValid(_ []byte) bool {
	return true
}

type KeyValidator struct{}

func (v KeyValidator) IsValid(key []byte) bool {
	if len(key) == 0 {
		return false
	}

	for _, c := range key {
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
