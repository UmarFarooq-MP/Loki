package entry

import "time"

// RecordType defines different WAL record actions.
type RecordType uint8

const (
	RecordPlace RecordType = iota
	RecordMatch
	RecordCancel
)

type Record struct {
	Type RecordType
	Time int64
	Data []byte
}

// NewRecord creates a new record for writing to WAL.
func NewRecord(t RecordType, data []byte) *Record {
	return &Record{
		Type: t,
		Time: time.Now().UnixNano(),
		Data: data,
	}
}
