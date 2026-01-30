package storage

import "os"

func createDirectory(directory string) error {
	return os.MkdirAll(directory, 0755)
}
