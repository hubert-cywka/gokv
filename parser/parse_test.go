package parser

import (
	"bytes"
	"kv/command"
	"kv/test/assert"
	"testing"
)

func TestParseCoreCommands(t *testing.T) {
	registerTestCommandDefinitions()

	tests := []struct {
		name        string
		input       string
		wantCommand *command.Command
		wantError   error
	}{
		{
			name:  "SET simple",
			input: "SET foo bar",
			wantCommand: &command.Command{
				Keyword:   "SET",
				Arguments: [][]byte{[]byte("foo"), []byte("bar")},
			},
		},
		{
			name:  "SET with spaces in value",
			input: "SET foo 'hello world'",
			wantCommand: &command.Command{
				Keyword:   "SET",
				Arguments: [][]byte{[]byte("foo"), []byte("hello world")},
			},
		},
		{
			name:      "SET missing value",
			input:     "SET foo",
			wantError: InvalidNumberOfTokens,
		},
		{
			name:  "GET valid",
			input: "GET foo",
			wantCommand: &command.Command{
				Keyword:   "GET",
				Arguments: [][]byte{[]byte("foo")},
			},
		},
		{
			name:      "GET missing key",
			input:     "GET",
			wantError: InvalidNumberOfTokens,
		},
		{
			name:      "GET invalid key",
			input:     "GET !",
			wantError: InvalidArgument,
		},
		{
			name:  "DELETE valid",
			input: "DELETE foo",
			wantCommand: &command.Command{
				Keyword:   "DELETE",
				Arguments: [][]byte{[]byte("foo")},
			},
		},
		{
			name:      "DELETE missing key",
			input:     "DELETE",
			wantError: InvalidNumberOfTokens,
		},
		{
			name:  "TXBEGIN",
			input: "TXBEGIN",
			wantCommand: &command.Command{
				Keyword:   "TXBEGIN",
				Arguments: [][]byte{},
			},
		},
		{
			name:  "TXCOMMIT",
			input: "TXCOMMIT",
			wantCommand: &command.Command{
				Keyword:   "TXCOMMIT",
				Arguments: [][]byte{},
			},
		},
		{
			name:  "TXABORT",
			input: "TXABORT",
			wantCommand: &command.Command{
				Keyword:   "TXABORT",
				Arguments: [][]byte{},
			},
		},
		{
			name:      "TXinvalid",
			input:     "TXFOO",
			wantError: InvalidCommandError,
		},
		{
			name:      "unknown command",
			input:     "FOO bar",
			wantError: InvalidCommandError,
		},
		{
			name:      "empty string",
			input:     "",
			wantError: InvalidCommandError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd, err := Parse(tt.input)

			if tt.wantError != nil {
				assert.Error(t, err, tt.wantError)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, cmd.Keyword, tt.wantCommand.Keyword)
			assertArguments(t, cmd.Arguments, tt.wantCommand.Arguments)
		})
	}
}

func assertArguments(t *testing.T, got, want [][]byte) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("expected %d arguments, got %d", len(want), len(got))
	}

	for i := range got {
		if !bytes.Equal(got[i], want[i]) {
			t.Fatalf("argument %d: expected %q, got %q", i, want[i], got[i])
		}
	}
}

func registerTestCommandDefinitions() {
	command.RegisterDefinition(&command.Definition{
		Keyword: "SET",
		Arguments: []command.ArgumentDefinition{
			{Validator: command.KeyValidator{}},
			{Validator: command.AnyValidator{}},
		},
		Handler: handleNoOp,
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
		Handler: handleNoOp,
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
		Handler: handleNoOp,
		Meta: command.Meta{
			Name:        "DELETE",
			Usage:       "DELETE <key>",
			Description: "Delete a key",
		},
	})

	command.RegisterDefinition(&command.Definition{
		Keyword:   "TXBEGIN",
		Arguments: nil,
		Handler:   handleNoOp,
		Meta: command.Meta{
			Name:        "TXBEGIN",
			Usage:       "TXBEGIN",
			Description: "Start a new transaction",
		},
	})

	command.RegisterDefinition(&command.Definition{
		Keyword:   "TXCOMMIT",
		Arguments: nil,
		Handler:   handleNoOp,
		Meta: command.Meta{
			Name:        "TXCOMMIT",
			Usage:       "TXCOMMIT",
			Description: "Commit current transaction",
		},
	})

	command.RegisterDefinition(&command.Definition{
		Keyword:   "TXABORT",
		Arguments: nil,
		Handler:   handleNoOp,
		Meta: command.Meta{
			Name:        "TXABORT",
			Usage:       "TXABORT",
			Description: "Abort current transaction",
		},
	})
}

func handleNoOp(tx command.TxControl, _ command.Store, _ *command.Command) command.ExecutionResult {
	return command.ExecutionResult{TxID: tx.CurrentTxID(), Err: nil}
}
