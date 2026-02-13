package storage

import "os"

func EnsureDirectoryExists(directory string) error {
	return os.MkdirAll(directory, 0755)
}
