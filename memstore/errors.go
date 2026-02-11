package memstore

import "errors"

var KeyNotFoundError = errors.New("memstore: key not found")
var SerializationError = errors.New("memstore: serialization error")
