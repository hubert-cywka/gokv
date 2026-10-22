package wal

import "errors"

var WriteAheadLogClosedError = errors.New("wal: closed")
