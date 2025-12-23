package main

import (
	"context"
	"fmt"
	"log"
	"net"
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
	// ---------------- Entry WAL ----------------

	entryWAL, err := entrywal.Open(entrywal.Config{
		Dir:             "./wal_entry",
		SegmentSize:     2 * 1024 * 1024,
		SegmentDuration: time.Minute,
	})
	if err != nil {
		log.Fatalf("entry WAL init failed: %v", err)
	}

	// ---------------- Exit WAL ----------------

	exitWAL, err := exitwal.Open("./wal_exit")
	if err != nil {
		log.Fatalf("exit WAL init failed: %v", err)
	}
	defer exitWAL.Close()

	// ---------------- Sequencer ----------------

	seqGen := sequence.New(0)

	// ---------------- Memory ----------------

	pool := memory.NewPool(func() *orderbook.Order {
		return &orderbook.Order{}
	})
	ring := memory.NewRetireRing(1 << 18)
	reader := snapshot.NewReader()

	// ---------------- Domain ----------------

	book := orderbook.NewOrderBook()

	// ---------------- WAL REPLAY ----------------

	if err := service.ReplayFromWAL(
		"./wal_entry",
		book,
		pool,
		seqGen,
	); err != nil {
		log.Fatalf("WAL replay failed: %v", err)
	}

	// ---------------- Service ----------------

	svc := service.NewOrderService(
		book,
		pool,
		ring,
		reader,
		seqGen,
		entryWAL,
		exitWAL,
	)

	// ---------------- Background Jobs ----------------

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			svc.AdvanceEpoch()
		}
	}()

	bc := broadcaster.New(exitWAL, 2*time.Second)
	go bc.Run(ctx)

	// ---------------- gRPC ----------------

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("listen failed: %v", err)
	}

	grpcSrv := grpc.NewServer()
	pb.RegisterOrderServiceServer(
		grpcSrv,
		grpcserver.NewServer(svc),
	)

	fmt.Println("🚀 Loki Engine running on :50051")

	if err := grpcSrv.Serve(lis); err != nil {
		log.Fatalf("gRPC server exited: %v", err)
	}
}
