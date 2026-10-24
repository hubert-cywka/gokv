package tx

import "errors"

var TransactionNotActiveError = errors.New("tx: transaction not active")
var MaxActiveTransactionsExceededError = errors.New("tx: max active transactions reached")
