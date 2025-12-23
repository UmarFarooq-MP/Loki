package service

import (
	"fmt"

	"loki/domain/orderbook"
	"loki/infra/memory"
	"loki/infra/sequence"
	entrywal "loki/infra/wal/entry"
	exitwal "loki/infra/wal/exit"
	"loki/snapshot"
)

/*
OrderService — single write entrypoint.

Order of operations (NON-NEGOTIABLE):
1) seq := Sequencer.Next()
2) Entry WAL append (with seq)
3) Execute matching
4) Exit WAL register
5) Respond to client
*/

type OrderService struct {
	book   *orderbook.OrderBook
	pool   *memory.Pool[orderbook.Order]
	ring   *memory.RetireRing
	reader *snapshot.Reader

	seqGen   *sequence.Sequencer
	entryWAL *entrywal.WAL
	exitWAL  *exitwal.ExitWAL
}

func NewOrderService(
	book *orderbook.OrderBook,
	pool *memory.Pool[orderbook.Order],
	ring *memory.RetireRing,
	reader *snapshot.Reader,
	seqGen *sequence.Sequencer,
	entryWAL *entrywal.WAL,
	exitWAL *exitwal.ExitWAL,
) *OrderService {
	return &OrderService{
		book:     book,
		pool:     pool,
		ring:     ring,
		reader:   reader,
		seqGen:   seqGen,
		entryWAL: entryWAL,
		exitWAL:  exitWAL,
	}
}

// -------------------- COMMAND --------------------

func (s *OrderService) PlaceOrder(
	side orderbook.Side,
	otype orderbook.OrderType,
	price int64,
	qty int64,
	userID uint64,
) uint64 {
	// 1️⃣ Generate sequence ID
	seq := s.seqGen.Next()

	// 2️⃣ Persist intent (ENTRY WAL)
	err := s.entryWAL.Append(
		entrywal.NewRecord(
			entrywal.RecordPlace,
			seq,
			[]byte(fmt.Sprintf(
				"%d|%d|%d|%d|%d",
				userID,
				side,
				otype,
				price,
				qty,
			)),
		),
	)
	if err != nil {
		panic(fmt.Errorf("entry WAL append failed: %w", err))
	}

	// 3️⃣ Execute matching
	o := s.pool.Get()
	*o = orderbook.Order{
		ID:     seq,
		Side:   side,
		Type:   otype,
		Price:  price,
		Qty:    qty,
		SeqID:  seq,
		Status: orderbook.Active,
	}

	s.book.Place(o)

	// 4️⃣ Register OUTBOX (EXIT WAL)
	if err := s.exitWAL.PutNew(seq); err != nil {
		fmt.Printf("[WARN] exit WAL write failed for seq %d: %v\n", seq, err)
	}

	// 5️⃣ Retire if filled
	if o.Remaining() == 0 {
		s.retire(o)
	}

	return seq
}

// -------------------- QUERY --------------------

func (s *OrderService) Snapshot() []*orderbook.Order {
	s.reader.Begin()
	defer s.reader.End()

	out := make([]*orderbook.Order, 0, 1024)

	s.book.BidsWalk(func(lvl *orderbook.PriceLevel) {
		for o := lvl.Head(); o != nil; o = o.Next() {
			if o.Status == orderbook.Active {
				out = append(out, o)
			}
		}
	})

	s.book.AsksWalk(func(lvl *orderbook.PriceLevel) {
		for o := lvl.Head(); o != nil; o = o.Next() {
			if o.Status == orderbook.Active {
				out = append(out, o)
			}
		}
	})

	return out
}

// -------------------- RECLAMATION --------------------

func (s *OrderService) AdvanceEpoch() {
	memory.AdvanceEpochAndReclaim(
		s.ring,
		s.pool,
		s.reader.Epoch(),
	)
}

func (s *OrderService) retire(o *orderbook.Order) {
	o.Status = orderbook.Inactive
	_ = s.ring.Enqueue(o)
}
