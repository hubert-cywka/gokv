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

		req, sessionErr := decode[queryRequest](r)
		if sessionErr != nil {
			_ = encode(w, http.StatusBadRequest, map[string]string{"error": sessionErr.Error()})
			return
		}

		if len(req.Statements) == 0 {
			_ = encode(w, http.StatusBadRequest, map[string]string{"error": "statements cannot be empty"})
			return
		}

		commands, parseErr := parser.ParseBulk(req.Statements)
		if parseErr != nil {
			_ = encode(w, http.StatusBadRequest, map[string]string{"error": parseErr.Error()})
			return
		}

		session, sessionErr := query.NewSession(txManager, kvStore, req.TxID)
		if sessionErr != nil {
			status := http.StatusInternalServerError
			if errors.Is(sessionErr, tx.TransactionNotActiveError) {
				status = http.StatusBadRequest
			}
			_ = encode(w, status, map[string]string{"error": sessionErr.Error()})
			return
		}

		results := executeCommands(session, commands)
		_ = encode(w, http.StatusOK, queryResponse{Results: results})
	}

	return http.HandlerFunc(handler)
}

func executeCommands(session *query.Session, commands []*command.Command) []queryResult {
	queryResults := make([]queryResult, len(commands))

	execResults := session.ExecuteBulk(commands)
	for i, execResult := range execResults {
		queryResults[i].OK = execResult.Err == nil
		queryResults[i].TxID = execResult.TxID

		if execResult.Value != nil {
			value := string(execResult.Value)
			queryResults[i].Value = &value
		}

		if execResult.Err != nil {
			queryResults[i].Error = execResult.Err.Error()
		}
	}

	return queryResults
}
