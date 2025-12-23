package entry

import (
	"encoding/binary"
	"io"
	"os"
)

// maxSeqInSegment scans a WAL segment and returns the maximum sequence ID found.
// It is used ONLY for snapshot-based truncation.
func maxSeqInSegment(path string) (uint64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	var max uint64

	for {
		// Header: [type:1][seq:8][time:8][len:4]
		header := make([]byte, 21)
		if _, err := io.ReadFull(f, header); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return max, nil
			}
			return max, err
		}

		seq := binary.BigEndian.Uint64(header[1:9])
		if seq > max {
			max = seq
		}

		payloadLen := binary.BigEndian.Uint32(header[17:21])

		// Skip payload + CRC
		if _, err := f.Seek(int64(payloadLen+4), io.SeekCurrent); err != nil {
			return max, err
		}
	}
}
