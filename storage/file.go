package storage

import "io"

type File interface {
	io.Writer
	io.Seeker
	io.Closer
	io.Reader
	Sync() error
}
