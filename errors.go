package main

import "errors"

var (
	ErrKeyTooLong   = errors.New("key too long")
	ErrValueTooLong = errors.New("value too long")
)
