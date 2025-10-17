package main

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
)

// Reader sequentially reads WAL records.
type Reader struct {
	file   *os.File
	reader *bufio.Reader
	rec    *Record
	err    error
	ser    Serializer
}

func OpenReader(path string, ser Serializer) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	if ser == nil {
		ser = BinarySerializer{}
	}
	return &Reader{file: f, reader: bufio.NewReaderSize(f, 1<<20), ser: ser}, nil
}

func (r *Reader) Next() bool {
	lengthBuf := make([]byte, 4)
	if _, err := io.ReadFull(r.reader, lengthBuf); err != nil {
		r.err = err
		return false
	}
	length := binary.LittleEndian.Uint32(lengthBuf)

	crcBuf := make([]byte, 4)
	if _, err := io.ReadFull(r.reader, crcBuf); err != nil {
		r.err = err
		return false
	}
	wantCRC := binary.LittleEndian.Uint32(crcBuf)

	data := make([]byte, length)
	if _, err := io.ReadFull(r.reader, data); err != nil {
		r.err = err
		return false
	}

	gotCRC := crc32.ChecksumIEEE(data)
	if gotCRC != wantCRC {
		r.err = errors.New("wal: crc mismatch")
		return false
	}

	rec, err := r.ser.Decode(data)
	if err != nil {
		r.err = err
		return false
	}
	r.rec = rec
	return true
}

func (r *Reader) Record() *Record { return r.rec }
func (r *Reader) Err() error      { return r.err }
func (r *Reader) Close() error    { return r.file.Close() }
