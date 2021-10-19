#!/bin/bash
imageName="producer-test"
docker build . -t consumer/"$imageName" &&
  docker run \
    --network kafka_kafka-network \
    consumer/"$imageName"
