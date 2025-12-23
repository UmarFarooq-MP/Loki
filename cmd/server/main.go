package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

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
	// ---------------- Memory & Domain ----------------

	book := orderbook.NewOrderBook()

	pool := memory.NewPool(func() *orderbook.Order {
		return &orderbook.Order{}
	})

	ring := memory.NewRetireRing(1 << 18)

	// ---------------- Sequencer ----------------

	seqGen := sequence.New(0)

	// ---------------- Entry WAL ----------------

	entryWAL, err := entrywal.Open(entrywal.Config{
		Dir:             "./wal_entry",
		SegmentSize:     2 * 1024 * 1024, // 2MB (tune later)
		SegmentDuration: time.Minute,
	})
	if err != nil {
		log.Fatalf("failed to open entry WAL: %v", err)
	}

	// ---------------- Exit WAL ----------------

	exitWAL, err := exitwal.Open("./wal_exit")
	if err != nil {
		log.Fatalf("failed to open exit WAL: %v", err)
	}
	defer exitWAL.Close()

	// ---------------- Snapshot LOAD ----------------

	snapSeq, err := snapshot.Load(
		"./snapshots/snapshot.bin",
		book,
		pool,
	)
	if err != nil {
		log.Fatalf("snapshot load failed: %v", err)
	}

	// ---------------- WAL REPLAY ----------------

	if err := service.ReplayFromWAL(
		"./wal_entry",
		book,
		pool,
		seqGen,
	); err != nil {
		log.Fatalf("WAL replay failed: %v", err)
	}

	// ---------------- Sequencer FIXUP ----------------

	if snapSeq > seqGen.Current() {
		seqGen.Reset(snapSeq)
	}

	fmt.Printf(
		"startup complete (snapshot_seq=%d, wal_seq=%d)\n",
		snapSeq,
		seqGen.Current(),
	)

	// ---------------- Service ----------------

	reader := snapshot.NewReader()

	svc := service.NewOrderService(
		book,
		pool,
		ring,
		reader,
		seqGen,
		entryWAL,
		exitWAL,
	)

	// ---------------- Snapshot rotation job ----------------

	svc.StartSnapshotJob(
		"./snapshots",
		30*time.Second,
	)

	// ---------------- Background jobs ----------------

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Epoch reclamation
	go func() {
		t := time.NewTicker(2 * time.Second)
		defer t.Stop()
		for range t.C {
			svc.AdvanceEpoch()
		}
	}()

	// Exit WAL broadcaster
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

	// DEV only (grpcurl convenience)
	reflection.Register(grpcSrv)

	fmt.Println("Loki engine running on :50051")

	if err := grpcSrv.Serve(lis); err != nil {
		log.Fatalf("grpc server exited: %v", err)
	}
}
