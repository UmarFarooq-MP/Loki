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

## To run Kakfa for testing
~~~bash
docker compose -f docker-compose.kafka.yaml up -d
~~~

Create a topic if not already created
~~~bash
docker exec -it kafka kafka-topics \
  --create \
  --topic orders \
  --bootstrap-server kafka:9092 \
  --partitions 1 \
  --replication-factor 1
~~~

To verify a topic
~~~bash
docker exec -it kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --list
~~~

To see realtime message publishing on Kafka
~~~bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic orders \
  --from-beginning
~~~