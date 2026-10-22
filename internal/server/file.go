package server

import (
	"os"
	"path/filepath"
)

func openFile(filename string, flag int) (*os.File, error) {
	dir := filepath.Dir(filename)

	if err := ensureDirectoryExists(dir); err != nil {
		return nil, err
	}

	file, err := os.OpenFile(filename, flag, 0644)

	if err != nil {
		return nil, err
	}

	return file, nil
}

func ensureDirectoryExists(directory string) error {
	return os.MkdirAll(directory, 0755)
}
