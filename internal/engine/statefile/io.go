package statefile

import "io"

type ioStream interface {
	io.Writer
	io.Seeker
	io.Closer
	io.Reader
	Sync() error
}
