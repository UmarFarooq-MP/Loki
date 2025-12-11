package wal

import (
	"encoding/json"
	"io"

	"google.golang.org/protobuf/proto"
)

// Serializer defines the interface for encoding and decoding WAL records.
type Serializer interface {
	Encode(*Record) ([]byte, error)
	Decoder(io.Reader) func() (*Record, error)
}

// ---------------- JSON Serializer ---------------- //

type JSONSerializer struct{}

func (JSONSerializer) Encode(r *Record) ([]byte, error) {
	return json.Marshal(r)
}

func (JSONSerializer) Decoder(r io.Reader) func() (*Record, error) {
	dec := json.NewDecoder(r)
	return func() (*Record, error) {
		var rec Record
		err := dec.Decode(&rec)
		return &rec, err
	}
}

// ---------------- Proto (gRPC) Serializer ---------------- //

type ProtoSerializer struct{}

func (ProtoSerializer) Encode(r *Record) ([]byte, error) {
	return proto.Marshal(r)
}

func (ProtoSerializer) Decoder(r io.Reader) func() (*Record, error) {
	return func() (*Record, error) {
		// Note: gRPC proto doesn’t have streaming decode natively.
		// You’ll usually read chunk-by-chunk during replay.
		data, err := io.ReadAll(r)
		if err != nil {
			return nil, err
		}
		rec := &Record{}
		if err := proto.Unmarshal(data, rec); err != nil {
			return nil, err
		}
		return rec, nil
	}
}
