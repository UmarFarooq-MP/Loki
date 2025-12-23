package entry

import (
	"fmt"
	"os"
	"path/filepath"
)

type segment struct {
	file   *os.File
	offset int64
}

func openSegment(dir string, index int) (*segment, error) {
	path := filepath.Join(dir, fmt.Sprintf("segment-%06d.wal", index))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &segment{file: f}, nil
}

func (s *segment) append(b []byte) error {
	n, err := s.file.Write(b)
	if err != nil {
		return err
	}
	s.offset += int64(n)
	return nil
}

func (s *segment) close() error {
	return s.file.Close()
}
