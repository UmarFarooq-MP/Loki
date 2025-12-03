package rbq

import (
	"loki/order_book"
	"testing"
)

func TestRetireRingBasic(t *testing.T) {
	r := newRetireRing(4) // capacity 4
	o1 := &order_book.Order{ID: 1}
	o2 := &order_book.Order{ID: 2}

	if !r.Enqueue(o1) || !r.Enqueue(o2) {
		t.Fatal("enqueue failed unexpectedly")
	}
	if r.Dequeue() != o1 {
		t.Error("expected first dequeue to be o1")
	}
	if r.Dequeue() != o2 {
		t.Error("expected second dequeue to be o2")
	}
	if r.Dequeue() != nil {
		t.Error("expected empty ring to return nil")
	}
}
