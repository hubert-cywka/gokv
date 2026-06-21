package http_server

import (
	"context"
	"kv/engine/tx"
	"kv/kvstore"
	"net/http"
)

func NewServer(txManager *tx.Manager, kvStore *kvstore.KVStore, ctx context.Context) http.Handler {
	mux := http.NewServeMux()

	queryHandler := createQueryHandler(txManager, kvStore, ctx)
	mux.Handle("/query", queryHandler)

	return mux
}
