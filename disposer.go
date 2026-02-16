package main

import "io"

type Disposer struct {
	closers []io.Closer
}

func (c *Disposer) Track(closer io.Closer) {
	c.closers = append(c.closers, closer)
}

func (c *Disposer) Dispose() error {
	var firstErr error

	for i := len(c.closers) - 1; i >= 0; i-- {
		if err := c.closers[i].Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}
