package main

import (
	"fmt"
	"kv/kv_store"
	"kv/mem_store"
	"kv/wal"
)

func main() {
	writeAheadLog, _ := wal.NewWriteAheadLog("./wal.log")
	defer writeAheadLog.Close()
	records, _ := writeAheadLog.Read()

	store := mem_store.NewMemStore()

	for _, record := range records {
		switch record.Kind() {
		case wal.Update:
			_ = store.Set(string(record.Key()), record.Value())
		case wal.Delete:
			_ = store.Delete(string(record.Key()))
		}
	}

	kvStore := kv_store.NewKeyValueStore(store, writeAheadLog)

	v1, _ := kvStore.Get("key1")
	v2, _ := kvStore.Get("key2")
	v3, _ := kvStore.Get("key3")

	kvStore.Set("key1", []byte("111"))
	kvStore.Set("key2", []byte("222"))
	kvStore.Set("key3", []byte("333"))

	fmt.Println(string(v1))
	fmt.Println(string(v2))
	fmt.Println(string(v3))
}
