Plain simple in-memory KV store. Supports MVCC and transactions. Copy of data is persisted on disk through write-ahead log.

Run `go run . --repl` to start REPL.

```
AVAILABLE COMMANDS
─────────────────────────────────────────────────────────────────────────────────────
Name                     | Usage                   | Description
─────────────────────────────────────────────────────────────────────────────────────
- TXBEGIN                   TXBEGIN                   Start a new transaction
- TXCOMMIT                  TXCOMMIT                  Commit current transaction
- TXABORT                   TXABORT                   Abort current transaction
- GET                       GET <key>                 Get value of a key
- SET                       SET <key> <value>         Set value for a key
- DELETE                    DELETE <key>              Delete a key
```
