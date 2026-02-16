package wal

import (
	"bytes"
	"fmt"
	"kv/engine/wal/record"
	"kv/observability"
	"kv/storage/mocks"
	"kv/test"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestWriteAheadLog_Append(t *testing.T) {
	observability.DisableLogging()

	commitWaitTime := time.Millisecond
	opts := Options{
		BatchCommitWaitTime: commitWaitTime,
		WriterBufferSize:    4096,
	}

	t.Run("it commits record after the wait time", func(t *testing.T) {
		file := mocks.NewFile()
		wal := NewWriteAheadLog(opts, file)
		errChan := make(chan error)

		value := []byte("value")

		go func() {
			errChan <- wal.Append(record.NewValue("key", value, 1))
		}()

		assertNotSynced(t, file)
		awaitSync(t, errChan, commitWaitTime*2)
		assertSyncedTimes(t, file, 1)
		assertFileContains(t, file, value)
	})

	t.Run("it batches commits", func(t *testing.T) {
		file := mocks.NewFile()
		wal := NewWriteAheadLog(opts, file)

		count := 100
		var wg sync.WaitGroup
		wg.Add(count)

		value := func(i int) []byte {
			return []byte(fmt.Sprintf("value-%d", i))
		}

		for i := 0; i < count; i++ {
			go func(id int) {
				defer wg.Done()
				_ = wal.Append(record.NewValue("key-"+strconv.Itoa(id), value(id), 1))
			}(i)
		}

		wg.Wait()

		assertSyncedTimes(t, file, 1)
		for i := 0; i < count; i++ {
			assertFileContains(t, file, value(i))
		}
	})

	t.Run("it returns error if already closed", func(t *testing.T) {
		file := mocks.NewFile()
		wal := NewWriteAheadLog(opts, file)

		_ = wal.Close()
		err := wal.Append(record.NewValue("key", []byte("value"), 1))

		test.AssertError(t, err, WriteAheadLogClosedError)
	})
}

func TestWriteAheadLog_Replay(t *testing.T) {
	commitWaitTime := time.Millisecond
	opts := Options{
		BatchCommitWaitTime: commitWaitTime,
		WriterBufferSize:    4096,
	}

	t.Run("it reads all commited records", func(t *testing.T) {
		file := mocks.NewFile()
		wal := NewWriteAheadLog(opts, file)

		record1 := record.NewValue("key1", []byte("value1"), 1)
		record2 := record.NewValue("key2", []byte("value2"), 1)
		_ = wal.Append(record1)
		_ = wal.Append(record2)

		got := make([]*record.Record, 0)
		replayFunc := func(r record.Record) {
			got = append(got, &r)
		}

		err := wal.Replay(replayFunc)

		test.AssertEqual(t, err, nil)
		test.AssertEqual(t, len(got), 2)
		test.AssertEqual(t, got[0], record1)
		test.AssertEqual(t, got[1], record2)
	})

	t.Run("it replays records in the same order", func(t *testing.T) {
		file := mocks.NewFile()
		wal := NewWriteAheadLog(opts, file)

		_ = wal.Append(record.NewValue("key1", []byte("value1"), 1))
		_ = wal.Append(record.NewValue("key2", []byte("value2"), 1))
		_ = wal.Append(record.NewValue("key3", []byte("value3"), 1))

		firstReplayResult := make([]*record.Record, 0)
		secondReplayResult := make([]*record.Record, 0)

		_ = wal.Replay(func(record record.Record) {
			firstReplayResult = append(firstReplayResult, &record)
		})

		_ = wal.Replay(func(record record.Record) {
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
	opts := Options{
		BatchCommitWaitTime: commitWaitTime,
		WriterBufferSize:    4096,
	}

	t.Run("it waits until pending batch is commited", func(t *testing.T) {
		file := mocks.NewFile()
		wal := NewWriteAheadLog(opts, file)

		value := []byte("value")

		go func() {
			_ = wal.Append(record.NewValue("key", value, 1))
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

func assertFileContains(t *testing.T, file *mocks.File, value []byte) {
	t.Helper()

	if !bytes.Contains(file.Data, value) {
		t.Errorf("expected log to contain value %v, but it was not found", value)
	}
}

func assertSyncedTimes(t *testing.T, file *mocks.File, times int) {
	t.Helper()

	if file.SyncCalls != times {
		t.Errorf("expected file to be synced %d times, instead it was synced %d times", times, file.SyncCalls)
	}
}

func assertNotSynced(t *testing.T, file *mocks.File) {
	t.Helper()

	if file.SyncCalls != 0 {
		t.Error("expected file not to be synced")
	}
}
