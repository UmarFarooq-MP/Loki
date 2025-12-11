package wal

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"

	"google.golang.org/protobuf/reflect/protoreflect"
)

const (
	RecordPlace  = 1
	RecordMatch  = 2
	RecordCancel = 3
)

type Record struct {
	Type int    `protobuf:"varint,1,opt,name=type,proto3"`
	Time int64  `protobuf:"varint,2,opt,name=time,proto3"`
	Data []byte `protobuf:"bytes,3,opt,name=data,proto3"`
}

func (r Record) ProtoReflect() protoreflect.Message {
	// NOOP
	return nil
}

func EncodeRecord(rec *Record) ([]byte, error) {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, int32(rec.Type))
	binary.Write(buf, binary.LittleEndian, rec.Time)
	binary.Write(buf, binary.LittleEndian, uint32(len(rec.Data)))
	buf.Write(rec.Data)
	sum := crc32.ChecksumIEEE(buf.Bytes())
	binary.Write(buf, binary.LittleEndian, sum)
	return buf.Bytes(), nil
}

func DecodeRecord(r io.Reader) (*Record, error) {
	var typ int32
	var ts int64
	var n uint32
	if err := binary.Read(r, binary.LittleEndian, &typ); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.LittleEndian, &ts); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.LittleEndian, &n); err != nil {
		return nil, err
	}

	data := make([]byte, n)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, err
	}

	var crc uint32
	if err := binary.Read(r, binary.LittleEndian, &crc); err != nil {
		return nil, err
	}
	computed := crc32.ChecksumIEEE(append([]byte{}, data...))
	if computed != crc {
		return nil, io.ErrUnexpectedEOF
	}
	return &Record{Type: int(typ), Time: ts, Data: data}, nil
}
