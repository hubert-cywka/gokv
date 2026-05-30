Plain simple in-memory KV store. Supports MVCC and transactions. Copy of data is persisted on disk through 
write-ahead log.

Run `go run .` to start REPL:
```
AVAILABLE COMMANDS
─────────────────────────────────────────────────────────────────────────────────────
Name                     | Usage                   | Description
─────────────────────────────────────────────────────────────────────────────────────
- TRANSACTION BEGIN         TRANSACTION BEGIN         Start a new transaction
- TRANSACTION COMMIT        TRANSACTION COMMIT        Commit current transaction
- TRANSACTION ABORT         TRANSACTION ABORT         Abort current transaction
- GET                       GET <key>                 Get value of a key
- SET                       SET <key> <value>         Set value for a key
- DELETE                    DELETE <key>              Delete a key
- HELP                      HELP                      Show help message
- EXIT                      EXIT                      Exit the REPL
```