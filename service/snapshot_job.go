package service

import (
	"time"

	"loki/snapshot"
)

func (s *OrderService) StartSnapshotJob(
	dir string,
	interval time.Duration,
) {
	w := &snapshot.Writer{Dir: dir}

	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()

		for range t.C {
			seq := s.seqGen.Current()

			// Write snapshot
			if err := w.Write(seq, s.book); err != nil {
				continue
			}

			// Truncate ENTRY WAL after snapshot
			_ = s.entryWAL.TruncateBefore(seq)

			// GC EXIT WAL (acked only)
			_ = s.exitWAL.TruncateAckedUpTo(seq)
		}
	}()
}
