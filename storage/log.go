package storage

import (
	"fmt"
	"io"
	"path/filepath"

	"github.com/rs/zerolog/log"
)

const initialSegmentsBufferSize = 16

type LogOptions struct {
	LogsDirectory string
	SegmentSize   int64
}

type Log struct {
	options            LogOptions
	segments           []*Segment
	activeSegmentIndex int
}

func NewLog(options LogOptions) (*Log, error) {
	c := &Log{
		options:            options,
		segments:           make([]*Segment, initialSegmentsBufferSize),
		activeSegmentIndex: 0,
	}

	if err := createDirectory(options.LogsDirectory); err != nil {
		return nil, err
	}

	err := c.bootstrap()
	return c, err
}

func (l *Log) Write(p []byte) (n int, err error) {
	for len(p) > 0 {
		space, err := l.segment().Space()
		if err != nil {
			return n, err
		}

		if space <= 0 {
			if err := l.loadSegment(l.activeSegmentIndex + 1); err != nil {
				return n, err
			}

			space, err = l.segment().Space()

			if err != nil {
				return n, err
			}
		}

		toWrite := len(p)
		if int64(toWrite) > space {
			toWrite = int(space)
		}

		written, err := l.segment().Write(p[:toWrite])
		if err != nil {
			return n, err
		}

		n += written
		p = p[written:]
	}
	return n, nil
}

func (l *Log) Read(p []byte) (n int, err error) {
	for len(p) > 0 {
		read, readErr := l.segment().Read(p)
		n += read
		p = p[read:]

		if readErr == io.EOF {

			if !l.hasNextSegment() {
				return n, io.EOF
			}

			if err := l.loadSegment(l.activeSegmentIndex + 1); err != nil {
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

	return l.segment().Seek(offset, whence)
}

func (l *Log) Close() error {
	if l.segment() == nil {
		return nil
	}

	return l.segment().Close()
}

func (l *Log) Sync() error {
	return l.segment().Sync()
}

func (l *Log) grow() {
	newCapacity := cap(l.segments) * 2
	newSegments := make([]*Segment, newCapacity)

	copy(newSegments, l.segments)
	l.segments = newSegments

	log.Info().
		Int("newCapacity", newCapacity).
		Msg("log: grow completed.")
}

func (l *Log) segment() *Segment {
	return l.segments[l.activeSegmentIndex]
}

func (l *Log) hasNextSegment() bool {
	nextIndex := l.activeSegmentIndex + 1

	if len(l.segments) <= nextIndex {
		return false
	}

	if l.segments[nextIndex] == nil {
		return false
	}

	return true
}

func (l *Log) bootstrap() error {
	log.Debug().
		Msg("log: bootstrapping.")

	for {
		nextIndex := l.activeSegmentIndex + 1

		if err := l.loadSegment(nextIndex); err != nil {
			return err
		}

		space, err := l.segment().Space()

		if err != nil {
			return err
		}

		log.Info().
			Int("activeSegmentIndex", nextIndex).
			Msg("log: loaded next segment.")

		if space > 0 {
			log.Info().
				Int("activeSegmentIndex", nextIndex).
				Msg("log: found active segment.")

			return nil
		}
	}
}

func (l *Log) loadSegment(index int) error {
	if l.segment() != nil {
		err := l.segment().Close()

		if err != nil {
			return err
		}
	}

	l.activeSegmentIndex = index

	for cap(l.segments) <= index {
		l.grow()
	}

	if l.segments[index] != nil {
		return nil
	}

	path := l.getSegmentPath(index)
	l.segments[index] = NewSegment(path, l.options.SegmentSize)

	return nil
}

func (l *Log) getSegmentPath(index int) string {
	filename := fmt.Sprintf("wal-%09d.log", index)
	return filepath.Join(l.options.LogsDirectory, filename)
}
