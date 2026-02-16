package wal

import (
	"bufio"
	"io"
	"kv/engine/wal/record"
	"kv/storage"
	"sync"
	"time"
)

// TODO: Snapshotting

type batchCommitContext struct {
	done chan struct{}
	err  error
}

type WriteAheadLog struct {
	file   storage.File
	closed bool

	writer *bufio.Writer
	mutex  sync.Mutex

	encoder *record.Encoder
	decoder *record.Decoder

	batch   *batchCommitContext
	options Options
}

type Options struct {
	BatchCommitWaitTime time.Duration
	WriterBufferSize    int
}

func NewWriteAheadLog(options Options, file storage.File) *WriteAheadLog {
	bufferedWriter := bufio.NewWriterSize(file, options.WriterBufferSize)

	return &WriteAheadLog{
		file:    file,
		writer:  bufferedWriter,
		encoder: record.NewEncoder(bufferedWriter),
		decoder: record.NewDecoder(file),
		options: options,
	}
}

func (w *WriteAheadLog) Append(record *record.Record) error {
	w.mutex.Lock()

	if w.closed {
		w.mutex.Unlock()
		return WriteAheadLogClosedError
	}

	if err := w.encoder.Encode(record); err != nil {
		w.mutex.Unlock()
		return err
	}

	if w.batch == nil {
		w.batch = &batchCommitContext{done: make(chan struct{})}
		time.AfterFunc(w.options.BatchCommitWaitTime, w.finalizeBatchCommit)
	}

	currentBatch := w.batch
	w.mutex.Unlock()

	<-currentBatch.done
	return currentBatch.err
}

func (w *WriteAheadLog) Replay(apply func(record.Record)) error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	for {
		var r record.Record

		if err := w.decoder.Decode(&r); err != nil {
			if err == io.EOF {
				break
			}

			return err
		}

		apply(r)
	}

	_, err := w.file.Seek(0, io.SeekEnd)
	return err
}

func (w *WriteAheadLog) Close() error {
	w.mutex.Lock()

	if w.closed {
		w.mutex.Unlock()
		return nil
	}

	w.closed = true
	activeBatch := w.batch
	w.mutex.Unlock()

	if activeBatch != nil {
		<-activeBatch.done
	}

	w.mutex.Lock()
	defer w.mutex.Unlock()

	if err := w.commit(); err != nil {
		_ = w.file.Close()
		return err
	}

	return w.file.Close()
}

func (w *WriteAheadLog) commit() error {
	if err := w.writer.Flush(); err != nil {
		return err
	}

	return w.file.Sync()
}

func (w *WriteAheadLog) finalizeBatchCommit() {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	if w.batch == nil {
		return
	}

	activeBatch := w.batch
	w.batch = nil

	err := w.commit()

	activeBatch.err = err
	close(activeBatch.done)
}
