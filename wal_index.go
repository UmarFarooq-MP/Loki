package main

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
)

func AppendIndexEntry(dir string, e WalIndexEntry) error {
	path := filepath.Join(dir, "wal_index.json")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	data, _ := json.Marshal(e)
	_, err = f.Write(append(data, '\n'))
	return err
}

func LoadLastIndex(dir string) (*WalIndexEntry, error) {
	path := filepath.Join(dir, "wal_index.json")
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	var last WalIndexEntry
	for scanner.Scan() {
		var e WalIndexEntry
		if err := json.Unmarshal(scanner.Bytes(), &e); err != nil {
			continue
		}
		// skip any stray current.wal entries
		if e.File == "current.wal" || e.File == "" {
			continue
		}
		last = e
	}
	if last.File == "" {
		return nil, nil
	}
	return &last, nil
}

// LoadAllIndex loads all WAL index entries.
func LoadAllIndex(dir string) ([]WalIndexEntry, error) {
	path := filepath.Join(dir, "wal_index.json")
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	var entries []WalIndexEntry
	for scanner.Scan() {
		var e WalIndexEntry
		if err := json.Unmarshal(scanner.Bytes(), &e); err != nil {
			continue
		}
		if e.File == "current.wal" || e.File == "" {
			continue
		}
		entries = append(entries, e)
	}
	return entries, nil
}
