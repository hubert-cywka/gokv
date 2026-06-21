package repl_server

import (
	"bufio"
	"context"
	"fmt"
	"kv/command"
	"kv/engine/query"
	"kv/engine/tx"
	"kv/kvstore"
	"kv/parser"
	"os"

	"github.com/rs/zerolog/log"
)

func Start(txManager *tx.Manager, kvStore *kvstore.KVStore, ctx context.Context) error {
	session, err := query.NewSession(txManager, kvStore, nil)
	if err != nil {
		return err
	}

	log.Info().Msg("repl server started")

	printHelp()
	fmt.Println("Enter a command:")
	fmt.Println()

	reader := bufio.NewScanner(os.Stdin)

	lines := make(chan string)
	scanErr := make(chan error, 1)

	go func() {
		defer close(lines)

		for reader.Scan() {
			lines <- reader.Text()
		}

		scanErr <- reader.Err()
		close(scanErr)
	}()

	for {
		fmt.Print("> ")

		select {
		case <-ctx.Done():
			return ctx.Err()

		case err := <-scanErr:
			if err != nil {
				return err
			}
			return nil

		case line, ok := <-lines:
			if !ok {
				return nil
			}

			cmd, err := parser.Parse(line)
			if err != nil {
				fmt.Println("ERR:", err)
				continue
			}

			result := session.Execute(cmd)
			if result.Err != nil {
				fmt.Println("ERR:", result.Err)
				continue
			}

			switch {
			case cmd.Keyword == "GET":
				if result.Value == nil {
					fmt.Println("(nil)")
				} else {
					fmt.Println(string(result.Value))
				}

			case cmd.Keyword == "TXBEGIN" && result.TxID != nil:
				fmt.Printf("OK (tx %d started)\n", *result.TxID)

			default:
				fmt.Println("OK")
			}
		}
	}
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
