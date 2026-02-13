package kvstore

import "errors"

var (
	ErrKeyTooLong   = errors.New("key too long")
	ErrValueTooLong = errors.New("value too long")
)
