package repl

import (
	"bufio"
	"fmt"
	"kv/command"
	"kv/engine/query"
	"kv/engine/tx"
	"kv/kvstore"
	"kv/parser"
	"os"
)

func Start(txManager *tx.Manager, kvStore *kvstore.KVStore) error {
	reader := bufio.NewScanner(os.Stdin)

	session, err := query.NewSession(txManager, kvStore, nil, query.ExecutionOptions{AbortTransactionOnError: false})
	if err != nil {
		return err
	}

	fmt.Println()
	fmt.Println("KV server started.")
	printHelp()
	fmt.Println("Enter a command:")
	fmt.Println()

	running := true

	for running {
		fmt.Print("> ")

		if !reader.Scan() {
			return reader.Err()
		}

		cmd, err := parser.Parse(reader.Text())
		if err != nil {
			fmt.Println("ERR:", err)
			continue
		}

		result := session.Execute(cmd)
		if result.Err != nil {
			fmt.Println("ERR:", result.Err)
			continue
		}

		if cmd.Keyword == "GET" {
			if result.Value == nil {
				fmt.Println("(nil)")
			} else {
				fmt.Println(string(result.Value))
			}
			continue
		}

		if cmd.Keyword == "TXBEGIN" && result.TxID != nil {
			fmt.Printf("OK (tx %d started)\n", *result.TxID)
			continue
		}

		fmt.Println("OK")
	}

	return nil
}

func printHelp() {
	fmt.Println()
	fmt.Println("AVAILABLE COMMANDS")
	fmt.Println("─────────────────────────────────────────────────────────────────────────────────────")
	fmt.Printf("> %-24s | %-23s | %s\n", "Name", "Usage", "Description")
	fmt.Println("─────────────────────────────────────────────────────────────────────────────────────")

	for _, definition := range command.Definitions() {
		meta := definition.Meta
		fmt.Printf("> %-24s | %-23s | %s\n", meta.Name, meta.Usage, meta.Description)
	}

	fmt.Println("─────────────────────────────────────────────────────────────────────────────────────")
	fmt.Println()
}
