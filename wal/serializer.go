package wal

import "errors"

type Serializer interface {
	Encode(*Record) ([]byte, error)
	Decode([]byte) (*Record, error)
}

var ErrCorruptRecord = errors.New("wal: corrupted record")

type BinarySerializer struct{}

func (BinarySerializer) Encode(rec *Record) ([]byte, error) {
	return rec.Data, nil
}

func (BinarySerializer) Decode(b []byte) (*Record, error) {
	if len(b) == 0 {
		return nil, ErrCorruptRecord
	}
	return &Record{Data: b}, nil
}
