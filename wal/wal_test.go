package wal

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"kv/test"
	"kv/wal/data"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestWriteAheadLog_Append(t *testing.T) {
	test.DisableLogging()

	commitWaitTime := time.Millisecond
	opts := WriteAheadLogOptions{
		BatchCommitWaitTime: commitWaitTime,
		WriterBufferSize:    4096,
	}

	t.Run("it commits record after the wait time", func(t *testing.T) {
		file := newMemFile()
		wal, _ := NewWriteAheadLog(opts, file)
		errChan := make(chan error)

		value := []byte("value")
		record := data.NewValueRecord("key", value)

		go func() {
			errChan <- wal.Append(record)
		}()

		assertNotSynced(t, file)
		awaitSync(t, errChan, commitWaitTime*2)
		assertSyncedTimes(t, file, 1)
		assertFileContains(t, file, value)
	})

	t.Run("it batches commits", func(t *testing.T) {
		file := newMemFile()
		wal, _ := NewWriteAheadLog(opts, file)

		count := 100
		var wg sync.WaitGroup
		wg.Add(count)

		value := func(i int) []byte {
			return []byte(fmt.Sprintf("value-%d", i))
		}

		for i := 0; i < count; i++ {
			go func(id int) {
				defer wg.Done()
				_ = wal.Append(data.NewValueRecord("key-"+strconv.Itoa(id), value(id)))
			}(i)
		}

		wg.Wait()

		assertSyncedTimes(t, file, 1)
		for i := 0; i < count; i++ {
			assertFileContains(t, file, value(i))
		}
	})

	t.Run("it returns error if already closed", func(t *testing.T) {
		file := newMemFile()
		wal, _ := NewWriteAheadLog(opts, file)

		_ = wal.Close()
		record := data.NewValueRecord("key", []byte("value"))
		err := wal.Append(record)

		test.AssertError(t, err, ErrWalClosed)
	})
}

func TestWriteAheadLog_Replay(t *testing.T) {
	commitWaitTime := time.Millisecond
	opts := WriteAheadLogOptions{
		BatchCommitWaitTime: commitWaitTime,
		WriterBufferSize:    4096,
	}

	t.Run("it reads all commited records", func(t *testing.T) {
		file := newMemFile()
		wal, _ := NewWriteAheadLog(opts, file)

		record1 := data.NewValueRecord("key1", []byte("value1"))
		record2 := data.NewValueRecord("key2", []byte("value2"))
		_ = wal.Append(record1)
		_ = wal.Append(record2)

		got := make([]*data.Record, 0)
		replayFunc := func(record data.Record) {
			got = append(got, &record)
		}

		err := wal.Replay(replayFunc)

		test.AssertEqual(t, err, nil)
		test.AssertEqual(t, len(got), 2)
		test.AssertEqual(t, got[0], record1)
		test.AssertEqual(t, got[1], record2)
	})

	t.Run("it replays records in the same order", func(t *testing.T) {
		file := newMemFile()
		wal, _ := NewWriteAheadLog(opts, file)

		_ = wal.Append(data.NewValueRecord("key1", []byte("value1")))
		_ = wal.Append(data.NewValueRecord("key2", []byte("value2")))
		_ = wal.Append(data.NewValueRecord("key3", []byte("value3")))

		firstReplayResult := make([]*data.Record, 0)
		secondReplayResult := make([]*data.Record, 0)

		_ = wal.Replay(func(record data.Record) {
			firstReplayResult = append(firstReplayResult, &record)
		})

		_ = wal.Replay(func(record data.Record) {
			secondReplayResult = append(secondReplayResult, &record)
		})

		test.AssertEqual(t, len(firstReplayResult), len(secondReplayResult))
		test.AssertEqual(t, firstReplayResult[0], secondReplayResult[0])
		test.AssertEqual(t, firstReplayResult[1], secondReplayResult[1])
		test.AssertEqual(t, firstReplayResult[2], secondReplayResult[2])
	})
}

func TestWriteAheadLog_Close(t *testing.T) {
	commitWaitTime := time.Millisecond
	opts := WriteAheadLogOptions{
		BatchCommitWaitTime: commitWaitTime,
		WriterBufferSize:    4096,
	}

	t.Run("it waits until pending batch is commited", func(t *testing.T) {
		file := newMemFile()
		wal, _ := NewWriteAheadLog(opts, file)

		value := []byte("value")
		record := data.NewValueRecord("key", value)

		go func() {
			_ = wal.Append(record)
		}()

		time.Sleep(commitWaitTime / 2)
		_ = wal.Close()

		assertFileContains(t, file, value)
	})
}

func awaitSync(t *testing.T, channel chan error, timeout time.Duration) {
	t.Helper()

	select {
	case err := <-channel:
		if err != nil {
			t.Error("expected sync not to fail")
		}
	case <-time.After(timeout):
		t.Fatal("expected sync not to timeout")
	}
}

func assertFileContains(t *testing.T, file *memFile, value []byte) {
	t.Helper()

	if !bytes.Contains(file.data, value) {
		t.Errorf("expected log to contain value %v, but it was not found", value)
	}
}

func assertSyncedTimes(t *testing.T, file *memFile, times int) {
	t.Helper()

	if file.syncCalls != times {
		t.Errorf("expected file to be synced %d times, instead it was synced %d times", times, file.syncCalls)
	}
}

func assertNotSynced(t *testing.T, file *memFile) {
	t.Helper()

	if file.syncCalls != 0 {
		t.Error("expected file not to be synced")
	}
}

type memFile struct {
	data      []byte
	offset    int64
	syncCalls int
	closed    bool
}

func newMemFile() *memFile {
	return &memFile{data: make([]byte, 0)}
}

func (m *memFile) Write(p []byte) (n int, err error) {
	if m.closed {
		return 0, errors.New("file closed")
	}

	lastBytePos := m.offset + int64(len(p))

	if lastBytePos > int64(len(m.data)) {
		newData := make([]byte, lastBytePos)
		copy(newData, m.data)
		m.data = newData
	}

	copy(m.data[m.offset:], p)
	m.offset += int64(len(p))
	return len(p), nil
}

func (m *memFile) Read(p []byte) (n int, err error) {
	if m.closed {
		return 0, errors.New("file closed")
	}
	if m.offset >= int64(len(m.data)) {
		return 0, io.EOF
	}

	n = copy(p, m.data[m.offset:])
	m.offset += int64(n)
	return n, nil
}

func (m *memFile) Seek(offset int64, whence int) (int64, error) {
	if m.closed {
		return 0, errors.New("file closed")
	}

	var newOffset int64
	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekCurrent:
		newOffset = m.offset + offset
	case io.SeekEnd:
		newOffset = int64(len(m.data)) + offset
	default:
		return 0, errors.New("invalid whence")
	}

	if newOffset < 0 {
		return 0, errors.New("negative position")
	}

	m.offset = newOffset
	return m.offset, nil
}

func (m *memFile) Sync() error {
	m.syncCalls++
	return nil
}

func (m *memFile) Close() error {
	m.closed = true
	return nil
}
