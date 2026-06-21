package query

func (s *Session) CurrentTxID() *uint64 {
	return s.currentTxID
}

func (s *Session) Begin() error {
	return s.begin()
}

func (s *Session) Commit() error {
	return s.commit()
}

func (s *Session) AbortTx() error {
	return s.abort()
}

func (s *Session) begin() error {
	if s.currentTx != nil {
		return ErrTransactionAlreadyActive
	}

	txHandle, err := s.txManager.BeginTx()
	if err != nil {
		return err
	}

	s.currentTx = txHandle
	txID := txHandle.ID.Uint64()
	s.currentTxID = &txID
	return nil
}

func (s *Session) commit() error {
	if !s.HasCurrentTx() {
		return ErrOperationOutsideTransaction
	}

	if err := s.currentTx.Commit(); err != nil {
		return err
	}

	s.currentTx = nil
	s.currentTxID = nil
	return nil
}

func (s *Session) abort() error {
	if !s.HasCurrentTx() {
		return ErrOperationOutsideTransaction
	}

	s.currentTx.Abort()
	s.currentTx = nil
	s.currentTxID = nil
	return nil
}
