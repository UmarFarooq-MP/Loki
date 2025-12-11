// Package service orchestrates the core components of the
// matching engine — orderbook, snapshotter, WAL, and memory.
//
// It provides a clean API for placing, cancelling, and
// querying orders, decoupled from network transports like gRPC.
package service
