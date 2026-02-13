package tx

import "errors"

var TransactionNotActiveError = errors.New("tx: transaction not activeTx")
var MaxActiveTransactionsExceededError = errors.New("tx: max activeTx transactions reached")
var ManifestChecksumMismatchError = errors.New("tx: checksum mismatch")
