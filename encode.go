package main

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
)

type BinarySerializer struct{}

func (BinarySerializer) Encode(rec *Record) ([]byte, error) {
	payload := new(bytes.Buffer)
	payload.WriteByte(byte(rec.Type))
	binary.Write(payload, binary.LittleEndian, rec.Seq)
	binary.Write(payload, binary.LittleEndian, rec.Time)
	payload.Write(rec.Data)

	body := payload.Bytes()
	crc := crc32.ChecksumIEEE(body)
	length := uint32(len(body))

	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, length)
	binary.Write(buf, binary.LittleEndian, crc)
	buf.Write(body)
	return buf.Bytes(), nil
}

func (BinarySerializer) Decode(data []byte) (*Record, error) {
	if len(data) < 17 {
		return nil, ErrCorruptRecord
	}
	rec := &Record{
		Type: RecordType(data[0]),
		Seq:  binary.LittleEndian.Uint64(data[1:9]),
		Time: int64(binary.LittleEndian.Uint64(data[9:17])),
		Data: data[17:],
	}
	return rec, nil
}
