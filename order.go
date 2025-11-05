package main

import (
	"bytes"
	"encoding/binary"
)

// --- Core Types ---

type OrderStatus uint8
type Side uint8
type OrderType uint8

const (
	Active OrderStatus = iota
	Inactive
)

const (
	Bid Side = iota
	Ask
)

const (
	Limit OrderType = iota
	Market
	IOC      // Immediate-Or-Cancel
	FOK      // Fill-Or-Kill
	PostOnly // Must not cross book
)

// Order represents a single order in the book
type Order struct {
	ID          uint64
	Side        Side
	Type        OrderType
	Price       int64
	Qty         int64
	Filled      int64
	SeqID       uint64
	Status      OrderStatus
	next, prev  *Order   // FIFO queue inside a price level
	retireEpoch uint64   // epoch when retired
	_           [32]byte // padding for cache line separation
}

// OrderPool: fixed-capacity stack pool (no GC churn in steady state)
type OrderPool struct {
	store []*Order
	top   int
}

func NewOrderPool(cap int) *OrderPool {
	p := &OrderPool{store: make([]*Order, cap), top: cap}
	for i := 0; i < cap; i++ {
		p.store[i] = new(Order)
	}
	return p
}

func (p *OrderPool) Get() *Order {
	if p.top == 0 {
		return nil // exhausted
	}
	p.top--
	o := p.store[p.top]
	*o = Order{Status: Active} // reset
	return o
}

func (p *OrderPool) Put(o *Order) {
	if p.top == len(p.store) {
		return // full
	}
	o.next, o.prev = nil, nil
	o.Status = Inactive
	p.store[p.top] = o
	p.top++
}

// --- WAL Integration Helpers ---

// EncodeBinary serializes an Order into a compact []byte format for WAL.
func (o *Order) EncodeBinary() []byte {
	buf := bytes.NewBuffer(make([]byte, 0, 64))
	_ = binary.Write(buf, binary.LittleEndian, o.ID)
	_ = binary.Write(buf, binary.LittleEndian, o.SeqID)
	_ = binary.Write(buf, binary.LittleEndian, o.Price)
	_ = binary.Write(buf, binary.LittleEndian, o.Qty)
	_ = binary.Write(buf, binary.LittleEndian, o.Filled)
	_ = binary.Write(buf, binary.LittleEndian, o.Side)
	_ = binary.Write(buf, binary.LittleEndian, o.Type)
	_ = binary.Write(buf, binary.LittleEndian, o.Status)
	return buf.Bytes()
}

// DecodeBinary reconstructs an Order from WAL binary data.
func DecodeBinary(data []byte) *Order {
	o := new(Order)
	buf := bytes.NewReader(data)
	_ = binary.Read(buf, binary.LittleEndian, &o.ID)
	_ = binary.Read(buf, binary.LittleEndian, &o.SeqID)
	_ = binary.Read(buf, binary.LittleEndian, &o.Price)
	_ = binary.Read(buf, binary.LittleEndian, &o.Qty)
	_ = binary.Read(buf, binary.LittleEndian, &o.Filled)
	_ = binary.Read(buf, binary.LittleEndian, &o.Side)
	_ = binary.Read(buf, binary.LittleEndian, &o.Type)
	_ = binary.Read(buf, binary.LittleEndian, &o.Status)
	return o
}
