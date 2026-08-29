Plain simple in-memory KV store. Supports MVCC and transactions. Copy of data is persisted on disk through write-ahead log.

# REPL

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

# HTTP API

Run `go run . --http` to start HTTP API server (default address: `:8080`).

HTTP endpoint:
- `POST /query`
- request body:
  - `statements` (`[]string`, required)
  - `txId` (`uint64`, optional)

Example:
```bash
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"statements":["TXBEGIN","SET key value","GET key", "TXCOMMIT"]}'
```

Response:
```json
{
  "results": [
    {"ok": true, "txId": 1},
    {"ok": true, "txId": 1},
    {"ok": true, "txId": 1, "value": "value"},
    {"ok": true}
  ]
}
```
