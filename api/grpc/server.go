package grpcserver

import (
	"context"
	"log"

	pb "loki/api/pb"
	"loki/orderbook"
	"loki/service"
)

type Server struct {
	pb.UnimplementedOrderServiceServer
	svc *service.OrderService
}

func NewServer(svc *service.OrderService) *Server {
	return &Server{svc: svc}
}

func (s *Server) PlaceOrder(ctx context.Context, req *pb.PlaceOrderRequest) (*pb.PlaceOrderResponse, error) {
	side := toSide(req.Side)
	otype := toType(req.Type)

	s.svc.PlaceOrder(side, otype, req.Price, req.Qty, req.UserId)
	log.Printf("[gRPC] Placed order: side=%v type=%v price=%d qty=%d", side, otype, req.Price, req.Qty)

	return &pb.PlaceOrderResponse{Status: "ok", SeqId: s.svc.Book.LastSeq.Load()}, nil
}

func (s *Server) CancelOrder(ctx context.Context, req *pb.CancelOrderRequest) (*pb.CancelOrderResponse, error) {
	log.Printf("[gRPC] Cancel request: order_id=%d price=%d", req.OrderId, req.Price)
	return &pb.CancelOrderResponse{Status: "ok"}, nil
}

func (s *Server) GetSnapshot(ctx context.Context, req *pb.SnapshotRequest) (*pb.SnapshotResponse, error) {
	orders := s.svc.Snapshot()
	resp := &pb.SnapshotResponse{}
	for _, o := range orders {
		resp.Orders = append(resp.Orders, &pb.OrderEntry{
			Id:    o.ID,
			Side:  fromSide(o.Side),
			Type:  fromType(o.Type),
			Price: o.Price,
			Qty:   o.Qty,
		})
	}
	return resp, nil
}

// --- converters ---
func toSide(s pb.Side) orderbook.Side {
	switch s {
	case pb.Side_BID:
		return orderbook.Bid
	case pb.Side_ASK:
		return orderbook.Ask
	default:
		return orderbook.Bid
	}
}

func toType(t pb.OrderType) orderbook.OrderType {
	switch t {
	case pb.OrderType_LIMIT:
		return orderbook.Limit
	case pb.OrderType_MARKET:
		return orderbook.Market
	case pb.OrderType_IOC:
		return orderbook.IOC
	case pb.OrderType_FOK:
		return orderbook.FOK
	case pb.OrderType_POST_ONLY:
		return orderbook.PostOnly
	default:
		return orderbook.Limit
	}
}

func fromSide(s orderbook.Side) pb.Side {
	if s == orderbook.Ask {
		return pb.Side_ASK
	}
	return pb.Side_BID
}

func fromType(t orderbook.OrderType) pb.OrderType {
	switch t {
	case orderbook.Limit:
		return pb.OrderType_LIMIT
	case orderbook.Market:
		return pb.OrderType_MARKET
	case orderbook.IOC:
		return pb.OrderType_IOC
	case orderbook.FOK:
		return pb.OrderType_FOK
	case orderbook.PostOnly:
		return pb.OrderType_POST_ONLY
	default:
		return pb.OrderType_LIMIT
	}
}
