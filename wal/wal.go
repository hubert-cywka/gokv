package wal

import (
	"bufio"
	"errors"
	"io"
	"kv/wal/record"
	"kv/wal/storage"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

// TODO: Support transaction commit

// TODO: Partitioned WAL
// - Quick append, slower replay.

// TODO: Compaction
// 1. Manifest file with metadata (e.g., 1st segment path).
// 2. Temporary files.
// 3. Clean up afterward.

var WriteAheadLogClosedError = errors.New("wal: closed")

type batchCommitContext struct {
	done chan struct{}
	err  error
}

type WriteAheadLogOptions struct {
	BatchCommitWaitTime time.Duration
	WriterBufferSize    int
}

type WriteAheadLog struct {
	file   storage.File
	closed bool

	writer *bufio.Writer
	mutex  sync.Mutex

	encoder *record.Encoder
	decoder *record.Decoder

	batch   *batchCommitContext
	options WriteAheadLogOptions
}

func NewWriteAheadLog(options WriteAheadLogOptions, file storage.File) (*WriteAheadLog, error) {
	bufferedWriter := bufio.NewWriterSize(file, options.WriterBufferSize)

	return &WriteAheadLog{
		file:    file,
		writer:  bufferedWriter,
		encoder: record.NewEncoder(bufferedWriter),
		decoder: record.NewDecoder(file),
		options: options,
	}, nil
}

func (w *WriteAheadLog) Append(record *record.Record) error {
	w.mutex.Lock()

	if w.closed {
		w.mutex.Unlock()
		return WriteAheadLogClosedError
	}

	if err := w.encoder.Encode(record); err != nil {
		w.mutex.Unlock()
		log.Error().
			Msg("wal: append failed")

		return err
	}

	if w.batch == nil {
		log.Debug().
			Msg("wal: starting batch commit")

		w.batch = &batchCommitContext{done: make(chan struct{})}
		time.AfterFunc(w.options.BatchCommitWaitTime, w.finalizeBatchCommit)
	} else {
		log.Debug().
			Msg("wal: joining batch commit")
	}

	currentBatch := w.batch
	w.mutex.Unlock()

	<-currentBatch.done
	return currentBatch.err
}

func (w *WriteAheadLog) Replay(apply func(record.Record)) error {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	log.Debug().
		Msg("wal: replaying")

	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		log.Error().
			Msg("wal: replay failed, couldn't seek")

		return err
	}

	for {
		var r record.Record

		if err := w.decoder.Decode(&r); err != nil {
			if err == io.EOF {
				log.Debug().
					Msg("wal: replayed all records")

				break
			}

			log.Error().
				Msg("wal: replay failed, couldn't read record")

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
		log.Error().
			Msg("wal: failed to commit before close")

		_ = w.file.Close()
		return err
	}

	log.Debug().
		Msg("wal: closed")

	return w.file.Close()
}

func (w *WriteAheadLog) commit() error {
	if err := w.writer.Flush(); err != nil {
		return err
	}

	log.Debug().
		Msg("wal: committed")

	return w.file.Sync()
}

func (w *WriteAheadLog) finalizeBatchCommit() {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	if w.batch == nil {
		log.Debug().
			Msg("wal: no active batch to commit")

		return
	}

	activeBatch := w.batch
	w.batch = nil

	err := w.commit()

	activeBatch.err = err
	close(activeBatch.done)
}
