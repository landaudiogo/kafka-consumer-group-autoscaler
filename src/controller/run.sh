docker build -t controller-image . && 
  docker run \
    -it --rm \
    --network kafka_kafka-network \
    --name controller-container \
    controller-image    
