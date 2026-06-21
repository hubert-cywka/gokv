package http_server

import (
	"context"
	"errors"
	"kv/command"
	"kv/engine/query"
	"kv/engine/tx"
	"kv/kvstore"
	"kv/parser"
	"net/http"
)

type queryRequest struct {
	Statements []string `json:"statements"`
	TxID       *uint64  `json:"txId,omitempty"`
}

type queryResult struct {
	OK    bool    `json:"ok"`
	TxID  *uint64 `json:"txId,omitempty"`
	Value *string `json:"value,omitempty"`
	Error string  `json:"error,omitempty"`
}

type queryResponse struct {
	Results []queryResult `json:"results"`
}

// TODO: Clean up, and make it a proper handler
func createQueryHandler(txManager *tx.Manager, kvStore *kvstore.KVStore, ctx context.Context) http.Handler {
	handler := func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		req, err := decode[queryRequest](r)
		if err != nil {
			_ = encode(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}

		if len(req.Statements) == 0 {
			_ = encode(w, http.StatusBadRequest, map[string]string{"error": "statements cannot be empty"})
			return
		}

		commands, parseErr := parseStatemens(req.Statements)
		if parseErr != nil {
			_ = encode(w, http.StatusBadRequest, map[string]string{"error": parseErr.Error()})
			return
		}

		session, err := query.NewSession(txManager, kvStore, req.TxID)
		if err != nil {
			status := http.StatusInternalServerError
			if errors.Is(err, tx.TransactionNotActiveError) {
				status = http.StatusBadRequest
			}
			_ = encode(w, status, map[string]string{"error": err.Error()})
			return
		}

		results := executeCommands(session, commands)
		_ = encode(w, http.StatusOK, queryResponse{Results: results})
	}

	return http.HandlerFunc(handler)
}

func parseStatemens(statements []string) ([]*command.Command, error) {
	commands := make([]*command.Command, len(statements))
	for i, statement := range statements {
		cmd, parseErr := parser.Parse(statement)
		if parseErr != nil {
			return nil, parseErr
		}
		commands[i] = cmd
	}

	return commands, nil
}

func executeCommands(session *query.Session, commands []*command.Command) []queryResult {
	results := make([]queryResult, len(commands))
	for i, cmd := range commands {
		execResult := session.Execute(cmd)
		results[i].OK = execResult.Err == nil
		results[i].TxID = execResult.TxID

		if execResult.Value != nil {
			value := string(execResult.Value)
			results[i].Value = &value
		}

		if execResult.Err != nil {
			results[i].Error = execResult.Err.Error()
		}
	}

	return results
}
