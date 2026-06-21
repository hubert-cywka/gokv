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
	if !s.hasCurrentTx() {
		return ErrOperationOutsideTransaction
	}

	return s.kvStore.Set(key, value, s.currentTx)
}

func (s *Session) get(key string) ([]byte, error) {
	if !s.hasCurrentTx() {
		return nil, ErrOperationOutsideTransaction
	}

	return s.kvStore.Get(key, s.currentTx)
}

func (s *Session) delete(key string) error {
	if !s.hasCurrentTx() {
		return ErrOperationOutsideTransaction
	}

	return s.kvStore.Delete(key, s.currentTx)
}
