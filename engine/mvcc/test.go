package mvcc

import (
	"kv/assert"
	"kv/engine/internal/mocks"
	"kv/engine/tx"
	storagemocks "kv/storage/mocks"
	"testing"
)

func AssertFrozen(t *testing.T, e *Version) {
	t.Helper()

	assert.Equal(t, e.XMin(), tx.IdFrozen)
}

func AssertNotFrozen(t *testing.T, e *Version) {
	t.Helper()

	assert.NotEqual(t, e.XMin(), tx.IdFrozen)
}

func AssertPruned(t *testing.T, e *Version) {
	t.Helper()

	assert.Equal(t, e, nil)
}

func AssertNotPruned(t *testing.T, e *Version) {
	t.Helper()

	assert.NotEqual(t, e, nil)
}

func beginTransaction(t *testing.T, txManager *tx.Manager) *tx.Transaction {
	transaction, err := txManager.BeginTx()
	assert.NoError(t, err)
	return transaction
}

func setupTxManager() *tx.Manager {
	file := storagemocks.NewFile()
	manifest := tx.NewManifest(file)
	writeAheadLog := mocks.NewAppender()

	return tx.NewManager(manifest, writeAheadLog, tx.ManagerOptions{
		ReservedIDsPerBatch:   1000,
		MaxActiveTransactions: 1000,
		TimeoutMs:             5000,
	})
}

func setup() (*Store, *VersionMap) {
	versionMap := NewVersionMap()
	return NewStore(versionMap), versionMap
}
