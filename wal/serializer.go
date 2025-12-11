package wal

import (
	"encoding/json"
	"errors"
	"io"

	"google.golang.org/protobuf/proto"
)

// Serializer defines the interface for encoding/decoding WAL records.
type Serializer interface {
	Encode(v any) ([]byte, error)
	Decoder(r io.Reader) func() (any, error)
}

// ---------------- JSON Serializer ---------------- //

type JSONSerializer struct{}

func (JSONSerializer) Encode(v any) ([]byte, error) {
	return json.Marshal(v)
}

func (JSONSerializer) Decoder(r io.Reader) func() (any, error) {
	dec := json.NewDecoder(r)
	return func() (any, error) {
		var v map[string]any
		err := dec.Decode(&v)
		return v, err
	}
}

// ---------------- Proto (gRPC) Serializer ---------------- //

type ProtoSerializer struct{}

func (ProtoSerializer) Encode(v any) ([]byte, error) {
	m, ok := v.(proto.Message)
	if !ok {
		return nil, ErrNotProto
	}
	return proto.Marshal(m)
}

func (ProtoSerializer) Decoder(r io.Reader) func() (any, error) {
	return func() (any, error) {
		data, err := io.ReadAll(r)
		if err != nil {
			return nil, err
		}
		var msg proto.Message
		if err := proto.Unmarshal(data, msg); err != nil {
			return nil, err
		}
		return msg, nil
	}
}

var ErrNotProto = errors.New("type does not implement proto.Message")
