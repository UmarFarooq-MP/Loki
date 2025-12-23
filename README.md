## To Generate Proto files
~~~bash
protoc --go_out=. --go-grpc_out=. api/pb/order.proto
~~~

## To place an order via `grpcurl` use this command

~~~bash
grpcurl -plaintext \
  -import-path api/pb \
  -proto order.proto \
  -d '{"side":"BID","type":"LIMIT","price":100,"qty":5,"user_id":1}' \
  localhost:50051 \
  loki.pb.OrderService/PlaceOrder
~~~