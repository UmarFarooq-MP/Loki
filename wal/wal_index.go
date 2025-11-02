package wal

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
)

// WalIndexEntry defines metadata for each WAL segment
type WalIndexEntry struct {
	File      string `json:"file"`
	FirstSeq  uint64 `json:"first_seq"`
	LastSeq   uint64 `json:"last_seq"`
	Timestamp string `json:"timestamp"`
}

// AppendIndexEntry adds a new segment entry to wal_index.json
func AppendIndexEntry(dir string, entry WalIndexEntry) error {
	path := filepath.Join(dir, "wal_index.json")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	data, _ := json.Marshal(entry)
	_, err = f.Write(append(data, '\n'))
	return err
}

// LoadAllIndex reads all WAL index entries from wal_index.json
func LoadAllIndex(dir string) ([]WalIndexEntry, error) {
	path := filepath.Join(dir, "wal_index.json")
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return []WalIndexEntry{}, nil // ✅ no index file = empty slice
		}
		return nil, err
	}

	lines := bytes.Split(b, []byte("\n"))
	var entries []WalIndexEntry
	for _, line := range lines {
		if len(line) == 0 {
			continue
		}
		var e WalIndexEntry
		if err := json.Unmarshal(line, &e); err == nil {
			entries = append(entries, e)
		}
	}
	return entries, nil
}

// LoadLastIndex returns the last segment entry, if any
func LoadLastIndex(dir string) (*WalIndexEntry, error) {
	index, err := LoadAllIndex(dir)
	if err != nil || len(index) == 0 {
		return nil, err
	}
	return &index[len(index)-1], nil
}
