package wal

import (
	"encoding/json"
	"errors"
	"io"

	"google.golang.org/protobuf/proto"
)

type Serializer interface {
	Encode(any) ([]byte, error)
	Decoder(io.Reader) func() (any, error)
}

// ---------- JSON ----------

type JSONSerializer struct{}

func (JSONSerializer) Encode(v any) ([]byte, error) {
	return json.Marshal(v)
}

func (JSONSerializer) Decoder(r io.Reader) func() (any, error) {
	dec := json.NewDecoder(r)
	return func() (any, error) {
		var m map[string]any
		err := dec.Decode(&m)
		return m, err
	}
}

// ---------- Protobuf ----------

type ProtoSerializer struct{}

var ErrNotProto = errors.New("value does not implement proto.Message")

func (ProtoSerializer) Encode(v any) ([]byte, error) {
	msg, ok := v.(proto.Message)
	if !ok {
		return nil, ErrNotProto
	}
	return proto.Marshal(msg)
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
