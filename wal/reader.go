package wal

import (
	"bufio"
	"os"
)

type WALReader struct {
	file   *os.File
	reader *bufio.Reader
	ser    Serializer
	rec    *Record
}

func OpenReader(path string, ser Serializer) (*WALReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &WALReader{
		file:   f,
		reader: bufio.NewReader(f),
		ser:    ser,
	}, nil
}

func (r *WALReader) Next() bool {
	data, err := r.reader.ReadBytes('\n')
	if err != nil {
		return false
	}
	rec, err := r.ser.Decode(data)
	if err != nil {
		return false
	}
	r.rec = rec
	return true
}

func (r *WALReader) Record() *Record {
	return r.rec
}

func (r *WALReader) Close() {
	_ = r.file.Close()
}
