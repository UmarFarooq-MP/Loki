package main

import "errors"

// Serializer defines how a Record is serialized/deserialized.
// You can plug in Binary, Protobuf, JSON, MsgPack, etc.
type Serializer interface {
	Encode(rec *Record) ([]byte, error)
	Decode(data []byte) (*Record, error)
}

var ErrCorruptRecord = errors.New("wal: corrupted record")
