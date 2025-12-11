package main

import (
	"fmt"
	"log"
	"net"
	"time"

	"google.golang.org/grpc"

	"loki/api/grpc"
	pb "loki/api/pb"
	"loki/boradcaster"
	"loki/memory"
	"loki/orderbook"
	"loki/rcu"
	"loki/service"
	"loki/snapshotter"
	"loki/wal/entry"
	"loki/wal/exit"
)

func main() {
	// --- Initialize WALs ---
	entryCfg := entry.Config{
		Dir:             "./wal_entry",
		SegmentSize:     2 * 1024 * 1024,
		SegmentDuration: time.Minute,
	}
	entryWAL, err := entry.New(entryCfg)
	if err != nil {
		log.Fatalf("failed to initialize entry WAL: %v", err)
	}

	exitWAL := exit.NewExitWAL()

	// --- Initialize memory + snapshot subsystems ---
	pool := memory.NewOrderPool(1_000_000)
	ring := memory.NewRetireRing(1 << 18) // 256K
	reader := &rcu.Reader{}

	// --- Initialize OrderBook ---
	book := orderbook.NewOrderBook()

	// --- Initialize service layer ---
	svc := service.NewOrderService(book, pool, ring, reader, entryWAL, exitWAL)

	// --- Initialize broadcaster (cron-style flush to Kafka) ---
	bc := broadcaster.NewBroadcaster(exitWAL)
	bc.StartCron(5 * time.Second)

	// --- Periodic reclaimer (snapshotter) ---
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		for range ticker.C {
			snapshotter.AdvanceEpochAndReclaim(ring, pool, reader)
		}
	}()

	// --- Start gRPC server ---
	grpcSrv := grpc.NewServer()
	pb.RegisterOrderServiceServer(grpcSrv, grpcserver.NewServer(svc))

	listener, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	fmt.Println("Loki Exchange Engine running on :50051")
	if err := grpcSrv.Serve(listener); err != nil {
		log.Fatalf("server exited: %v", err)
	}
}
