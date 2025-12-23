package wal

import "hash/crc32"

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

func CRC32Valid(data []byte, sum uint32) bool {
	return CRC32(data) == sum
}
