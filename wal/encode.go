package wal

type RecordType byte

const (
	RecordPlace    RecordType = 1
	RecordCancel   RecordType = 2
	RecordMatch    RecordType = 3
	RecordSnapshot RecordType = 4
)

type Record struct {
	Type RecordType
	Seq  uint64
	Time int64
	Data []byte
}
