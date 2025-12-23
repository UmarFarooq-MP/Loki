// Package snapshotter provides consistent, read-only access to
// the in-memory orderbook state. It defines lightweight readers
// that enter and exit read epochs safely, ensuring snapshots
// taken during concurrent matching are consistent without locks.
//
// Snapshotter is intentionally decoupled from order matching,
// write-ahead logging, and persistence. It only coordinates
// read visibility using the memory epoch model.
package snapshot
