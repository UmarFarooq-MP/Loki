// Package entry implements the ingress Write-Ahead Log (WAL).
// Every order or cancel request is recorded here before matching,
// guaranteeing durability even if the process crashes mid-processing.
package entry
