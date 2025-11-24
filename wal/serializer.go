package wal

import (
	"encoding/binary"
	"errors"
)

type Serializer interface {
	Encode(*Record) ([]byte, error)
	Decode([]byte) (*Record, error)
}

var ErrCorruptRecord = errors.New("wal: corrupted record")

type BinarySerializer struct{}

func (BinarySerializer) Encode(rec *Record) ([]byte, error) {
	dataLen := uint32(len(rec.Data))
	buf := make([]byte, 1+8+8+4+len(rec.Data))
	buf[0] = byte(rec.Type)
	binary.LittleEndian.PutUint64(buf[1:9], rec.Seq)
	binary.LittleEndian.PutUint64(buf[9:17], uint64(rec.Time))
	binary.LittleEndian.PutUint32(buf[17:21], dataLen)
	copy(buf[21:], rec.Data)
	return buf, nil
}

func (BinarySerializer) Decode(b []byte) (*Record, error) {
	if len(b) < 21 {
		return nil, ErrCorruptRecord
	}
	length := binary.LittleEndian.Uint32(b[17:21])
	end := 21 + int(length)
	if end > len(b) {
		return nil, ErrCorruptRecord
	}
	rec := &Record{
		Type: RecordType(b[0]),
		Seq:  binary.LittleEndian.Uint64(b[1:9]),
		Time: int64(binary.LittleEndian.Uint64(b[9:17])),
		Data: make([]byte, length),
	}
	copy(rec.Data, b[21:end])
	return rec, nil
}
