package storage

import "os"

func ensureDirectoryExists(directory string) error {
	return os.MkdirAll(directory, 0755)
}
