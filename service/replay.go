package service

import (
	"fmt"
	"strconv"
	"strings"

	"loki/domain/orderbook"
	"loki/infra/memory"
	"loki/infra/sequence"
	entrywal "loki/infra/wal/entry"
)

/*
ReplayFromWAL rebuilds in-memory state from Entry WAL.

IMPORTANT:
- This MUST run before accepting traffic
- Exit WAL is NOT replayed
*/

func ReplayFromWAL(
	walDir string,
	book *orderbook.OrderBook,
	pool *memory.Pool[orderbook.Order],
	seqGen *sequence.Sequencer,
) error {
	lastSeq, err := entrywal.Replay(walDir, func(rec *entrywal.Record) error {
		if rec.Type != entrywal.RecordPlace {
			return nil
		}

		// Payload format:
		// userID|side|type|price|qty
		parts := strings.Split(string(rec.Data), "|")
		if len(parts) != 5 {
			return fmt.Errorf("invalid WAL payload: %s", string(rec.Data))
		}

		// userID is intentionally ignored during replay
		_, err := strconv.ParseUint(parts[0], 10, 64)
		if err != nil {
			return err
		}

		side, err := strconv.Atoi(parts[1])
		if err != nil {
			return err
		}

		otype, err := strconv.Atoi(parts[2])
		if err != nil {
			return err
		}

		price, err := strconv.ParseInt(parts[3], 10, 64)
		if err != nil {
			return err
		}

		qty, err := strconv.ParseInt(parts[4], 10, 64)
		if err != nil {
			return err
		}

		o := pool.Get()
		*o = orderbook.Order{
			ID:     rec.Seq,
			Side:   orderbook.Side(side),
			Type:   orderbook.OrderType(otype),
			Price:  price,
			Qty:    qty,
			SeqID:  rec.Seq,
			Status: orderbook.Active,
		}

		book.Place(o)
		return nil
	})
	if err != nil {
		return err
	}

	// Resume sequencing AFTER replay
	seqGen.Reset(lastSeq)

	fmt.Printf("WAL replay completed successfully (last seq = %d)\n", lastSeq)
	return nil
}
