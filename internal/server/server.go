package server

import (
	"context"
	"fmt"
	"kv/internal/api/http_server"
	"kv/internal/api/repl_server"
	"kv/internal/config"
	"kv/internal/engine/mvcc"
	"kv/internal/engine/tx"
	"kv/internal/engine/vacuum"
	"kv/internal/engine/wal"
	"kv/internal/kvstore"
)

type Server struct {
	cfg  config.Config
	mode StartMode

	kvStore    *kvstore.KVStore
	versionMap *mvcc.VersionMap
	txManager  *tx.Manager
	vacuumer   *vacuum.Vacuumer
	wal        *wal.WriteAheadLog

	disposer Disposer
}

func New(cfg config.Config, mode StartMode) (app *Server, err error) {
	app = &Server{cfg: cfg, mode: mode}

	defer func() {
		if err != nil {
			_ = app.Close()
		}
	}()

	app.versionMap = mvcc.NewVersionMap()

	app.wal, err = newWriteAheadLog(cfg, &app.disposer)
	if err != nil {
		return nil, err
	}

	app.txManager, err = newTxManager(app.wal, cfg, &app.disposer)
	if err != nil {
		return nil, err
	}

	app.kvStore, err = newKVStore(app.versionMap, app.wal, app.wal, cfg)
	if err != nil {
		return nil, err
	}

	// TODO: "Smart" autovacuum - do not run it if there is no need to.
	app.vacuumer = vacuum.NewVacuumer(app.versionMap, app.wal)

	return app, nil
}

func (a *Server) Run(ctx context.Context) error {
	a.vacuumer.RunOnInterval(a.txManager, a.cfg.VacuumInterval, ctx)

	switch a.mode {
	case StartModeRepl:
		return repl_server.Start(a.txManager, a.kvStore, ctx)
	case StartModeHTTP:
		return http_server.Start(a.cfg.HTTPAddress, a.txManager, a.kvStore, ctx)
	default:
		return fmt.Errorf("unknown start mode: %v", a.mode)
	}
}

func (a *Server) Close() error {
	return a.disposer.Dispose()
}
