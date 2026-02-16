package wal

import (
	"fmt"
	"io"
	"kv/storage"
	"path/filepath"
)

const initialSegmentsBufferSize = 16

type LogOptions struct {
	LogsDirectory string
	SegmentSize   int64
}

type Log struct {
	options             LogOptions
	manifest            *Manifest
	segments            []*Segment
	activeSegmentOffset uint64
}

func NewLog(manifest *Manifest, options LogOptions) (*Log, error) {
	l := &Log{
		options:             options,
		segments:            make([]*Segment, initialSegmentsBufferSize),
		manifest:            manifest,
		activeSegmentOffset: 0,
	}

	if err := storage.EnsureDirectoryExists(options.LogsDirectory); err != nil {
		return nil, err
	}

	err := l.loadMostRecentSegment()
	return l, err
}

func (l *Log) Write(p []byte) (n int, err error) {
	var space int64
	var written int

	for len(p) > 0 {
		if space, err = l.activeSegment().Space(); err != nil {
			return n, err
		}

		if space <= 0 {
			if err = l.loadSegment(l.activeSegmentOffset + 1); err != nil {
				return n, err
			}

			if space, err = l.activeSegment().Space(); err != nil {
				return n, err
			}
		}

		toWrite := len(p)
		if int64(toWrite) > space {
			toWrite = int(space)
		}

		if written, err = l.activeSegment().Write(p[:toWrite]); err != nil {
			return n, err
		}

		n += written
		p = p[written:]
	}

	return n, nil
}

func (l *Log) Read(p []byte) (n int, err error) {
	for len(p) > 0 {
		read, readErr := l.activeSegment().Read(p)
		n += read
		p = p[read:]

		if readErr == io.EOF {
			if !l.hasNextSegment() {
				return n, io.EOF
			}

			if err = l.loadSegment(l.activeSegmentOffset + 1); err != nil {
				return n, err
			}

			continue
		}

		if readErr != nil {
			return n, readErr
		}

		if read == 0 {
			break
		}
	}
	return n, nil
}

func (l *Log) Seek(offset int64, whence int) (int64, error) {
	if whence == io.SeekStart && offset == 0 {
		if err := l.loadSegment(0); err != nil {
			return 0, err
		}
		return 0, nil
	}

	return l.activeSegment().Seek(offset, whence)
}

func (l *Log) Close() error {
	if l.activeSegment() != nil {
		err := l.activeSegment().Close()

		if err != nil {
			return err
		}
	}

	return nil
}

func (l *Log) Sync() error {
	return l.activeSegment().Sync()
}

func (l *Log) grow() {
	newCapacity := cap(l.segments) * 2
	newSegments := make([]*Segment, newCapacity)

	copy(newSegments, l.segments)
	l.segments = newSegments
}

func (l *Log) activeSegment() *Segment {
	return l.segments[l.activeSegmentOffset]
}

func (l *Log) hasNextSegment() bool {
	nextOffset := l.activeSegmentOffset + 1

	if uint64(len(l.segments)) <= nextOffset {
		return false
	}

	if l.segments[nextOffset] == nil {
		return false
	}

	return true
}

func (l *Log) loadMostRecentSegment() error {
	nextOffset := l.activeSegmentOffset

	for {
		if err := l.loadSegment(nextOffset); err != nil {
			return err
		}

		space, err := l.activeSegment().Space()

		if err != nil {
			return err
		}

		if space > 0 {
			return nil
		}

		nextOffset++
	}
}

func (l *Log) loadSegment(offset uint64) error {
	if l.activeSegment() != nil {
		err := l.activeSegment().Close()

		if err != nil {
			return err
		}
	}

	l.activeSegmentOffset = offset

	for uint64(cap(l.segments)) <= offset {
		l.grow()
	}

	if l.segments[offset] != nil {
		return nil
	}

	path, err := l.getSegmentPath(offset)

	if err != nil {
		return err
	}

	l.segments[offset] = NewSegment(path, l.options.SegmentSize)

	return nil
}

func (l *Log) getSegmentPath(offset uint64) (string, error) {
	seqNumber, err := l.findSegmentSequenceNumber(offset)

	if err != nil {
		return "", err
	}

	filename := fmt.Sprintf("wal-%09d.log", seqNumber)
	return filepath.Join(l.options.LogsDirectory, filename), nil
}

func (l *Log) findSegmentSequenceNumber(offset uint64) (uint64, error) {
	lsnStart, err := l.manifest.GetLogStart()

	if err != nil {
		return 0, err
	}

	return lsnStart + offset, nil
}
