package entry

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type ReplayHandler func(*Record) error

func Replay(dir string, fn ReplayHandler) (lastSeq uint64, err error) {
	files, err := filepath.Glob(filepath.Join(dir, "segment-*.wal"))
	if err != nil {
		return 0, err
	}

	for _, path := range files {
		f, err := os.Open(path)
		if err != nil {
			return lastSeq, err
		}

		for {
			rec, err := readRecord(f)
			if err != nil {
				if err == io.EOF {
					break
				}
				return lastSeq, err
			}

			if rec.Seq <= lastSeq {
				return lastSeq, fmt.Errorf("non-monotonic seq %d", rec.Seq)
			}
			lastSeq = rec.Seq

			if err := fn(rec); err != nil {
				return lastSeq, err
			}
		}
		_ = f.Close()
	}

	return lastSeq, nil
}

func readRecord(r io.Reader) (*Record, error) {
	header := make([]byte, 21)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, err
	}

	t := RecordType(header[0])
	seq := binary.BigEndian.Uint64(header[1:9])
	ts := binary.BigEndian.Uint64(header[9:17])
	l := binary.BigEndian.Uint32(header[17:21])

	data := make([]byte, l+4)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, err
	}

	payload := data[:l]
	crc := binary.BigEndian.Uint32(data[l:])

	if !CRC32Valid(append(header, payload...), crc) {
		return nil, fmt.Errorf("crc mismatch")
	}

	return &Record{
		Type: t,
		Seq:  seq,
		Time: int64(ts),
		Data: payload,
	}, nil
}
