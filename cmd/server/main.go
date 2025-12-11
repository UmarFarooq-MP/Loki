package main

import (
	"fmt"
	"log"
	"net"

	"loki/api/grpc"
	pb "loki/api/pb"
	"loki/service"

	"google.golang.org/grpc"
)

func main() {
	svc := service.NewOrderService(1<<20, 1<<16)
	server := grpcserver.NewServer(svc)

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterOrderServiceServer(s, server)

	fmt.Println("gRPC OrderService running on :50051")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("server exited: %v", err)
	}
}
