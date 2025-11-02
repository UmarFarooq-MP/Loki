package wal

import (
	"hash/crc32"
	"loki/wal/walpb"

	"google.golang.org/protobuf/proto"
)

// ProtoSerializer implements Serializer using Protobuf.
type ProtoSerializer struct{}

func (ProtoSerializer) Encode(rec *Record) ([]byte, error) {
	p := &walpb.PBRecord{
		Seq:  rec.Seq,
		Time: rec.Time,
		Data: rec.Data,
		Type: uint32(rec.Type),
	}
	body, err := proto.Marshal(p)
	if err != nil {
		return nil, err
	}
	crc := crc32.ChecksumIEEE(body)
	header := make([]byte, 8)
	putUint32LE(header[:4], uint32(len(body)))
	putUint32LE(header[4:], crc)
	return append(header, body...), nil
}

func (ProtoSerializer) Decode(data []byte) (*Record, error) {
	if len(data) < 8 {
		return nil, ErrCorruptRecord
	}
	body := data[8:]
	want := readUint32LE(data[4:])
	got := crc32.ChecksumIEEE(body)
	if want != got {
		return nil, ErrCorruptRecord
	}
	var pb walpb.PBRecord
	if err := proto.Unmarshal(body, &pb); err != nil {
		return nil, err
	}
	return &Record{
		Seq:  pb.Seq,
		Time: pb.Time,
		Data: pb.Data,
		Type: RecordType(pb.Type),
	}, nil
}

func putUint32LE(buf []byte, v uint32) {
	buf[0] = byte(v)
	buf[1] = byte(v >> 8)
	buf[2] = byte(v >> 16)
	buf[3] = byte(v >> 24)
}

func readUint32LE(buf []byte) uint32 {
	return uint32(buf[0]) |
		uint32(buf[1])<<8 |
		uint32(buf[2])<<16 |
		uint32(buf[3])<<24
}
