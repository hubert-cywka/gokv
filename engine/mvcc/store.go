package mvcc

import (
	"errors"
	"kv/engine/tx"
)

var KeyNotFoundError = errors.New("mvcc: key not found")
var SerializationError = errors.New("mvcc: serialization error")

type Store struct {
	versionMap *VersionMap
}

func NewStore(versionMap *VersionMap) *Store {
	return &Store{
		versionMap: versionMap,
	}
}

func (s *Store) Get(key string, t *tx.Transaction) ([]byte, error) {
	chain, ok := s.versionMap.GetChain(key)
	if !ok {
		return nil, KeyNotFoundError
	}

	rec := chain.FindVisible(t)
	if rec == nil || rec.Value == nil {
		return nil, KeyNotFoundError
	}

	return rec.Value, nil
}

func (s *Store) Set(key string, value []byte, t *tx.Transaction) error {
	chain := s.versionMap.GetOrCreateChain(key)

	for {
		latest := chain.Head()

		if err := s.tryUpdate(latest, t); err != nil {
			return err
		}

		newVersion := NewVersion(key, value, t.ID)
		newVersion.SetPreviousVersion(latest)

		if chain.CompareHeadAndSwap(latest, newVersion) {
			if latest != nil {
				t.Track(latest)
			}

			t.Track(newVersion)
			return nil
		}

		if latest != nil {
			latest.Resurrect()
		}
	}
}

func (s *Store) Delete(key string, t *tx.Transaction) error {
	chain, ok := s.versionMap.GetChain(key)
	if !ok {
		return KeyNotFoundError
	}

	latest := chain.Head()
	if latest == nil {
		return KeyNotFoundError
	}

	if err := s.tryDelete(latest, t); err != nil {
		return err
	}

	t.Track(latest)
	return nil
}

func (s *Store) tryUpdate(latest *Version, t *tx.Transaction) error {
	if latest == nil {
		return nil
	}

	xMax := latest.XMax()

	// Updates after own deletes are allowed
	if xMax == t.ID {
		return nil
	}

	if !xMax.IsAlive() {
		return SerializationError
	}

	if !t.CanSee(latest.XMin(), xMax) {
		return SerializationError
	}

	if !latest.TryKill(t.ID) {
		return SerializationError
	}

	return nil
}

func (s *Store) tryDelete(latest *Version, t *tx.Transaction) error {
	if latest == nil {
		return KeyNotFoundError
	}

	xMax := latest.XMax()

	if !xMax.IsAlive() {
		return SerializationError
	}

	if !t.CanSee(latest.XMin(), xMax) {
		return SerializationError
	}

	if !latest.TryKill(t.ID) {
		return SerializationError
	}

	return nil
}
