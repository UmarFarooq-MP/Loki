package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
)

type WALReader struct {
	file   *os.File
	reader *bufio.Reader
	ser    Serializer
	rec    *Record
	err    error
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
	var header [frameHeaderSize]byte
	if _, err := io.ReadFull(r.reader, header[:]); err != nil {
		if err == io.EOF {
			return false
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			r.err = ErrCorruptRecord
		} else {
			r.err = err
		}
		return false
	}
	length := binary.LittleEndian.Uint32(header[:4])
	payload := make([]byte, length)
	if _, err := io.ReadFull(r.reader, payload); err != nil {
		if err == io.EOF || errors.Is(err, io.ErrUnexpectedEOF) {
			r.err = ErrCorruptRecord
		} else {
			r.err = err
		}
		return false
	}
	checksum := binary.LittleEndian.Uint32(header[4:])
	if crc32.ChecksumIEEE(payload) != checksum {
		r.err = ErrCorruptRecord
		return false
	}
	rec, err := r.ser.Decode(payload)
	if err != nil {
		r.err = err
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

func (r *WALReader) Err() error {
	return r.err
}
