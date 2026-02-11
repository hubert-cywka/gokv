package storage

import (
	"fmt"
	"io"
	"path/filepath"

	"github.com/rs/zerolog/log"
)

const initialSegmentsBufferSize = 16

type LogOptions struct {
	ManifestPath  string
	LogsDirectory string
	SegmentSize   int64
}

type Log struct {
	options             LogOptions
	manifest            *Manifest
	segments            []*Segment
	activeSegmentOffset uint64
}

func NewLog(options LogOptions) (*Log, error) {
	c := &Log{
		options:             options,
		segments:            make([]*Segment, initialSegmentsBufferSize),
		activeSegmentOffset: 0,
	}

	if err := ensureDirectoryExists(options.LogsDirectory); err != nil {
		return nil, err
	}

	err := c.bootstrap()
	return c, err
}

func (l *Log) Write(p []byte) (n int, err error) {
	var space int64
	var written int

	for len(p) > 0 {
		if space, err = l.segment().Space(); err != nil {
			return n, err
		}

		if space <= 0 {
			if err = l.loadSegment(l.activeSegmentOffset + 1); err != nil {
				return n, err
			}

			if space, err = l.segment().Space(); err != nil {
				return n, err
			}
		}

		toWrite := len(p)
		if int64(toWrite) > space {
			toWrite = int(space)
		}

		if written, err = l.segment().Write(p[:toWrite]); err != nil {
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

	return l.segment().Seek(offset, whence)
}

func (l *Log) Close() error {
	if l.segment() != nil {
		err := l.segment().Close()

		if err != nil {
			return err
		}
	}

	if l.manifest != nil {
		return l.manifest.Close()
	}

	return nil
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
		Int("capacity", newCapacity).
		Msg("log: grow completed.")
}

func (l *Log) segment() *Segment {
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

func (l *Log) bootstrap() error {
	log.Debug().
		Msg("log: bootstrapping.")

	if err := l.loadManifest(); err != nil {
		return err
	}

	nextOffset := l.activeSegmentOffset

	for {
		if err := l.loadSegment(nextOffset); err != nil {
			return err
		}

		space, err := l.segment().Space()

		if err != nil {
			return err
		}

		sequenceNumber := l.findSegmentSequenceNumber(nextOffset)

		log.Info().
			Uint64("seq", sequenceNumber).
			Msg("log: loaded next segment.")

		if space > 0 {
			log.Info().
				Uint64("seq", sequenceNumber).
				Msg("log: found active segment.")

			return nil
		}

		nextOffset++
	}
}

func (l *Log) loadManifest() error {
	if l.manifest != nil {
		return nil
	}

	l.manifest = NewManifest(l.options.ManifestPath)
	return l.manifest.Open()
}

func (l *Log) loadSegment(offset uint64) error {
	if l.segment() != nil {
		err := l.segment().Close()

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

	path := l.getSegmentPath(offset)
	l.segments[offset] = NewSegment(path, l.options.SegmentSize)

	return nil
}

func (l *Log) getSegmentPath(offset uint64) string {
	seqNumber := l.findSegmentSequenceNumber(offset)
	filename := fmt.Sprintf("wal-%09d.log", seqNumber)
	return filepath.Join(l.options.LogsDirectory, filename)
}

func (l *Log) findSegmentSequenceNumber(offset uint64) uint64 {
	return l.manifest.Content.logStart + offset
}
