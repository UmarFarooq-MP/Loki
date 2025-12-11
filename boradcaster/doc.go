// Package forwarder implements a background job that periodically
// scans the exit WAL for unacknowledged records and publishes them
// to an external sink (like Kafka or NATS).
package broadcaster
