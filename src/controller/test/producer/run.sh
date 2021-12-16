docker build . -t controller/producer-image &&
  docker run \
    -it --rm \
    --name controller-producer-container \
    --network kafka_kafka-network \
    controller/producer-image
