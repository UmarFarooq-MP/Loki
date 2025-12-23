package wal

import "time"

// RecordType defines WAL intent.
type RecordType uint8

const (
	RecordPlace RecordType = iota
	RecordMatch
	RecordCancel
)

// Record is an immutable WAL entry.
type Record struct {
	Type RecordType
	Time int64
	Data []byte
}

func NewRecord(t RecordType, data []byte) *Record {
	return &Record{
		Type: t,
		Time: time.Now().UnixNano(),
		Data: data,
	}
}
