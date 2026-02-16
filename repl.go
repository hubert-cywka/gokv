package main

import (
	"bufio"
	"fmt"
	"kv/engine/tx"
	"kv/kvstore"
	"kv/query"
	"os"
)

// TOOD: Clean up
func startRepl(txManager *tx.Manager, kvStore *kvstore.KVStore) error {
	reader := bufio.NewScanner(os.Stdin)

	var currentTx *tx.Transaction

	fmt.Println("KV server started. Type commands (or 'HELP' for help):")

	running := true

	for running {
		fmt.Print("> ")

		if !reader.Scan() {
			return reader.Err()
		}

		cmd, err := query.Parse(reader.Text())
		if err != nil {
			fmt.Println("ERR:", err)
			continue
		}

		switch cmd.Type {

		case query.CommandExit:
			running = false
			break

		case query.CommandHelp:
			printHelp()

		case query.CommandBegin:
			if currentTx != nil {
				fmt.Println("ERR: transaction already active")
				continue
			}

			tx, err := txManager.Begin()
			if err != nil {
				fmt.Println("ERR:", err)
				continue
			}

			currentTx = tx
			fmt.Printf("OK (tx %d started)\n", tx.ID)

		case query.CommandCommit:
			if currentTx == nil {
				fmt.Println("ERR: no active transaction")
				continue
			}

			if err := currentTx.Commit(); err != nil {
				fmt.Println("ERR:", err)
				continue
			}

			currentTx = nil
			fmt.Println("OK")

		case query.CommandAbort:
			if currentTx == nil {
				fmt.Println("ERR: no active transaction")
				continue
			}

			currentTx.Abort()
			currentTx = nil
			fmt.Println("OK")

		case query.CommandSet:
			if currentTx == nil {
				fmt.Println("ERR: no active transaction")
				continue
			}

			if err := kvStore.Set(cmd.Key, cmd.Value, currentTx); err != nil {
				fmt.Println("ERR:", err)
				continue
			}

			fmt.Println("OK")

		case query.CommandGet:
			if currentTx == nil {
				fmt.Println("ERR: no active transaction")
				continue
			}

			val, err := kvStore.Get(cmd.Key, currentTx)
			if err != nil {
				fmt.Println("ERR:", err)
				continue
			}

			if val == nil {
				fmt.Println("(nil)")
			} else {
				fmt.Println(string(val))
			}

		case query.CommandDelete:
			if currentTx == nil {
				fmt.Println("ERR: no active transaction")
				continue
			}

			if err := kvStore.Delete(cmd.Key, currentTx); err != nil {
				fmt.Println("ERR:", err)
				continue
			}

			fmt.Println("OK")

		default:
			fmt.Println("ERR: unsupported command")
		}
	}

	return nil
}

func printHelp() {
	fmt.Println()
	fmt.Println("AVAILABLE COMMANDS")
	fmt.Println("─────────────────────────────────────────────────────────────────────────────────────")
	fmt.Printf("  %-24s | %-23s | %s\n", "Name", "Usage", "Description")
	fmt.Println("─────────────────────────────────────────────────────────────────────────────────────")

	order := []query.CommandType{
		query.CommandBegin,
		query.CommandCommit,
		query.CommandAbort,
		query.CommandGet,
		query.CommandSet,
		query.CommandDelete,
		query.CommandHelp,
		query.CommandExit,
	}

	for _, cmdType := range order {
		meta := query.CommandRegistry[cmdType]
		fmt.Printf("- %-25s %-25s %s\n", meta.Name, meta.Usage, meta.Description)
	}

	fmt.Println()
}
