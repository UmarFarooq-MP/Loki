package wal

import (
	"hash/crc32"
)

// CRC32Checksum computes a standard IEEE CRC-32 checksum for the data.
func CRC32Checksum(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

// CRC32Validate checks if the data matches the provided checksum.
func CRC32Validate(data []byte, sum uint32) bool {
	return crc32.ChecksumIEEE(data) == sum
}
