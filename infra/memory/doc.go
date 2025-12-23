// Package memory provides the low-level primitives for memory
// management and safe reclamation. It includes lock-free data
// structures such as OrderPool, RetireRing, and global epoch
// tracking used by the orderbook and snapshotter.
//
// The memory package is dependency-free and forms the foundation
// for concurrent object reuse and RCU-style epoch advancement.
package memory
