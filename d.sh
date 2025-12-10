#!/bin/bash
# ============================================
# Loki Exchange Engine Project Setup Script
# ============================================
# Creates directory structure and stub files
# for orderbook, snapshotter, WAL, gRPC, etc.
# ============================================

set -e

echo "🚀 Setting up project directories for 'loki'..."

# --- Core app entrypoint ---
mkdir -p cmd/engine

# --- Internal components ---
mkdir -p internal/{orderbook,snapshotter,wal/{inner,outer},orderservice,grpc/proto,common}

# --- Public reusable packages ---
mkdir -p pkg/{snapshotapi,walreader}

# --- Tests ---
mkdir -p tests

# --- Orderbook Core ---
touch internal/orderbook/{order_book.go,price_level.go,rb_tree.go,retire_ring.go,pool.go,types.go,README.md}

# --- Snapshotter ---
touch internal/snapshotter/{snapshotter.go,snapshot_reader.go,README.md}

# --- WAL System ---
touch internal/wal/inner/{wal.go,encode.go,reader.go,integration.go,README.md}
touch internal/wal/outer/{manager.go,serializer.go,README.md}
touch internal/wal/types.go

# --- Order Service Layer ---
touch internal/orderservice/{service.go,request_queue.go,coordinator.go,interfaces.go,README.md}

# --- gRPC Layer ---
touch internal/grpc/{server.go,interceptors.go,README.md}
touch internal/grpc/proto/{orderbook.proto,README.md}

# --- Common Utilities ---
touch internal/common/{logger.go,config.go,metrics.go,errors.go,README.md}

# --- Public APIs ---
touch pkg/snapshotapi/{api.go,README.md}
touch pkg/walreader/{reader.go,README.md}

# --- Test Suite ---
touch tests/{orderbook_test.go,wal_integration_test.go,snapshot_recovery_test.go,README.md}

# --- Entrypoint ---
cat <<'EOF' > cmd/engine/main.go
package main

import (
	"fmt"
)

func main() {
	fmt.Println("🚀 Loki Exchange Engine starting up...")
	// TODO: wire gRPC server + order service + snapshotter here
}
EOF

# --- Root .gitignore ---
cat <<'EOF' > .gitignore
# Build artifacts
bin/
*.exe
*.out

# Logs
*.log
wal_data/
snapshots/

# Dependencies
vendor/

# IDE
.idea/
.vscode/
EOF

echo "✅ Directory structure created successfully!"
echo ""
tree -d -L 3
