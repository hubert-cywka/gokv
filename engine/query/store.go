package query

func (s *Session) Set(key string, value []byte) error {
	return s.set(key, value)
}

func (s *Session) Get(key string) ([]byte, error) {
	return s.get(key)
}

func (s *Session) Delete(key string) error {
	return s.delete(key)
}

func (s *Session) set(key string, value []byte) error {
	if s.currentTx == nil {
		return ErrNoActiveTransaction
	}

	return s.kvStore.Set(key, value, s.currentTx)
}

func (s *Session) get(key string) ([]byte, error) {
	if s.currentTx == nil {
		return nil, ErrNoActiveTransaction
	}

	return s.kvStore.Get(key, s.currentTx)
}

func (s *Session) delete(key string) error {
	if s.currentTx == nil {
		return ErrNoActiveTransaction
	}

	return s.kvStore.Delete(key, s.currentTx)
}
