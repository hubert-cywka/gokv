package query

import (
	"kv/test"
	"testing"
)

func TestParse(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		wantCommand *Command
		wantError   error
	}{
		{
			name:  "SET simple",
			input: "SET foo bar",
			wantCommand: &Command{
				Type:  CommandSet,
				Key:   "foo",
				Value: []byte("bar"),
			},
		},
		{
			name:  "SET with spaces in value",
			input: "SET foo 'hello world'",
			wantCommand: &Command{
				Type:  CommandSet,
				Key:   "foo",
				Value: []byte("hello world"),
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
			wantCommand: &Command{
				Type: CommandGet,
				Key:  "foo",
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
			wantError: InvalidKeyError,
		},
		{
			name:  "DELETE valid",
			input: "DELETE foo",
			wantCommand: &Command{
				Type: CommandDelete,
				Key:  "foo",
			},
		},
		{
			name:      "DELETE missing key",
			input:     "DELETE",
			wantError: InvalidNumberOfTokens,
		},
		{
			name:  "TRANSACTION BEGIN",
			input: "TRANSACTION BEGIN",
			wantCommand: &Command{
				Type: CommandBegin,
			},
		},
		{
			name:  "TRANSACTION COMMIT",
			input: "TRANSACTION COMMIT",
			wantCommand: &Command{
				Type: CommandCommit,
			},
		},
		{
			name:  "TRANSACTION ABORT",
			input: "TRANSACTION ABORT",
			wantCommand: &Command{
				Type: CommandAbort,
			},
		},
		{
			name:      "TRANSACTION invalid",
			input:     "TRANSACTION FOO",
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
				test.AssertError(t, tt.wantError, err)
				return
			}

			test.AssertNoError(t, err)
			test.AssertEqual(t, cmd.Type, tt.wantCommand.Type)
			test.AssertEqual(t, cmd.Key, tt.wantCommand.Key)
			test.AssertBytesEqual(t, cmd.Value, tt.wantCommand.Value)
		})
	}
}
