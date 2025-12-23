package main

import (
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"loki/api/grpcserver"
	pb "loki/api/pb"

	"loki/domain/orderbook"

	"loki/infra/memory"
	"loki/infra/sequence"
	entrywal "loki/infra/wal/entry"
	exitwal "loki/infra/wal/exit"

	"loki/jobs/broadcaster"
	"loki/service"
	"loki/snapshot"
)

func main() {
	log.Println("starting loki engine")

	// -----------------------------
	// Domain
	// -----------------------------
	book := orderbook.NewOrderBook()

	// -----------------------------
	// Memory (REAL API)
	// -----------------------------
	pool := memory.NewPool(func() *orderbook.Order {
		return &orderbook.Order{}
	})
	ring := memory.NewRetireRing(2048)

	// -----------------------------
	// Sequencer
	// -----------------------------
	seqGen := sequence.New(0)

	// -----------------------------
	// Snapshot reader
	// -----------------------------
	snapReader := snapshot.NewReader()

	// -----------------------------
	// WALs
	// -----------------------------
	entryWAL, err := entrywal.Open(entrywal.Config{
		Dir:         "./data/wal/entry",
		SegmentSize: 64 << 20,
	})
	if err != nil {
		log.Fatalf("entry WAL open failed: %v", err)
	}

	exitWAL, err := exitwal.Open("./data/wal/exit")
	if err != nil {
		log.Fatalf("exit WAL open failed: %v", err)
	}

	// -----------------------------
	// Replay BEFORE serving
	// -----------------------------
	if err := service.ReplayFromWAL(
		"./data/wal/entry",
		book,
		pool,
		seqGen,
	); err != nil {
		log.Fatalf("WAL replay failed: %v", err)
	}

	// -----------------------------
	// Core service
	// -----------------------------
	orderSvc := service.NewOrderService(
		book,
		pool,
		ring,
		snapReader,
		seqGen,
		entryWAL,
		exitWAL,
	)

	// -----------------------------
	// Snapshot job (METHOD, not function)
	// -----------------------------
	orderSvc.StartSnapshotJob(
		"./data/snapshots",
		5*time.Second,
	)

	// -----------------------------
	// Broadcaster job (owns Kafka)
	// -----------------------------
	b, err := broadcaster.New(
		exitWAL,
		[]string{"localhost:29092"},
		"orders",
	)
	if err != nil {
		log.Fatalf("broadcaster init failed: %v", err)
	}
	go b.Run()

	// -----------------------------
	// gRPC server
	// -----------------------------
	grpcSrv := grpc.NewServer()
	pb.RegisterOrderServiceServer(
		grpcSrv,
		grpcserver.NewServer(orderSvc),
	)

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("listen failed: %v", err)
	}

	go func() {
		log.Println("loki engine running on :50051")
		if err := grpcSrv.Serve(lis); err != nil {
			log.Fatalf("grpc serve failed: %v", err)
		}
	}()

	// -----------------------------
	// Graceful shutdown
	// -----------------------------
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch

	log.Println("shutting down")
	grpcSrv.GracefulStop()
}
