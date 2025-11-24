package wal

import (
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
	return proto.Marshal(p)
}

func (ProtoSerializer) Decode(data []byte) (*Record, error) {
	var pb walpb.PBRecord
	if err := proto.Unmarshal(data, &pb); err != nil {
		return nil, err
	}
	return &Record{
		Seq:  pb.Seq,
		Time: pb.Time,
		Data: pb.Data,
		Type: RecordType(pb.Type),
	}, nil
}
