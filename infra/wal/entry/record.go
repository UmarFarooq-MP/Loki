package entry

import "time"

type RecordType uint8

const (
	RecordPlace RecordType = iota
	RecordCancel
)

type Record struct {
	Type RecordType
	Seq  uint64
	Time int64
	Data []byte
}

func NewRecord(t RecordType, seq uint64, data []byte) *Record {
	return &Record{
		Type: t,
		Seq:  seq,
		Time: time.Now().UnixNano(),
		Data: data,
	}
}
