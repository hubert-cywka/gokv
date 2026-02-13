package storage

import (
	"errors"
	"os"
	"path/filepath"
)

var FileNotOpenError = errors.New("file manager: file not open")

type Manager struct {
	files map[string]*os.File
}

func NewManager() *Manager {
	return &Manager{make(map[string]*os.File)}
}

func (fm *Manager) Open(filename string, flag int) (*os.File, error) {
	dir := filepath.Dir(filename)

	if err := EnsureDirectoryExists(dir); err != nil {
		return nil, err
	}

	file, err := fm.get(filename)

	if err == nil {
		return file, nil
	}

	file, err = os.OpenFile(filename, flag, 0644)

	if err != nil {
		return nil, err
	}

	fm.files[filename] = file
	return file, nil
}

func (fm *Manager) get(filename string) (*os.File, error) {
	file, ok := fm.files[filename]

	if !ok {
		return nil, FileNotOpenError
	}

	return file, nil
}
