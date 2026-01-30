package wal

import (
	"errors"
	"os"
	"sync"
)

type WriteAheadLog struct {
	file  *os.File
	mutex sync.RWMutex
}

func NewWriteAheadLog(path string) (*WriteAheadLog, error) {
	f, err := openLogFile(path)

	if err != nil {
		return nil, errors.New("failed to open log file")
	}

	return &WriteAheadLog{
		file:  f,
		mutex: sync.RWMutex{},
	}, nil
}

func (w *WriteAheadLog) Read() ([]Record, error) {
	w.mutex.RLock()
	defer w.mutex.RUnlock()

	_, err := w.file.Seek(0, 0)
	if err != nil {
		return nil, err
	}

	var records []Record

	for {
		record := Record{}
		ok := record.Read(w.file)

		if !ok {
			break
		}

		records = append(records, record)
	}

	return records, nil
}

func (w *WriteAheadLog) Append(record *Record) error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	record.Write(w.file)
	return w.flush()
}

func (w *WriteAheadLog) flush() error {
	return w.file.Sync()
}

func (w *WriteAheadLog) Close() error {
	return w.file.Close()
}

func openLogFile(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
}
