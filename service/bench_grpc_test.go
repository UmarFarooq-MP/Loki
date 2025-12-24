package service

import (
	"context"
	"loki/api/pb"
	"testing"

	"google.golang.org/grpc"
)

func BenchmarkGRPCPlaceOrder(b *testing.B) {
	conn, err := grpc.Dial(
		"localhost:50051",
		grpc.WithInsecure(),
	)
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close()

	client := pb.NewOrderServiceClient(conn)

	b.ResetTimer()
	b.RunParallel(func(pb2 *testing.PB) {
		for pb2.Next() {
			_, err := client.PlaceOrder(context.Background(), &pb.PlaceOrderRequest{
				Side:   pb.Side_BID,
				Type:   pb.OrderType_LIMIT,
				Price:  100,
				Qty:    1,
				UserId: 1,
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
