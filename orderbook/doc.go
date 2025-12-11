// Package orderbook implements the in-memory matching engine for
// limit, market, and special order types. It maintains two red-black
// trees for bid and ask sides, supports high-throughput matching,
// and integrates with the write-ahead log (WAL) and snapshotter.
//
// The orderbook operates as a single-writer system designed for
// extreme performance (>200k TPS) with lock-free reads using
// epoch-based memory reclamation.
package orderbook
