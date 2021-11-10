#!/bin/bash
docker build . -t huub/test-monitor &&
  docker run \
    -it --name de-monitor --rm \
    --add-host="ip-172-31-7-133.eu-west-1.compute.internal:52.213.38.208" \
    --add-host="ip-172-31-32-34.eu-west-1.compute.internal:18.202.250.11" \
    --add-host="ip-172-31-8-59.eu-west-1.compute.internal:54.171.156.36" \
    --add-host="ip-172-31-30-37.eu-west-1.compute.internal:54.76.46.203" \
    --network kafka_kafka-network \
    huub/test-monitor
