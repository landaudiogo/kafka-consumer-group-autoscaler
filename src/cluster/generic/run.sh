#!/bin/bash
docker build . -t consumer/generic &&
  docker run \
    -it --rm --name generic-container \
    --add-host="ip-172-31-7-133.eu-west-1.compute.internal:172.31.7.133" \
    --add-host="ip-172-31-32-34.eu-west-1.compute.internal:172.31.32.34" \
    --add-host="ip-172-31-30-37.eu-west-1.compute.internal:172.31.30.37" \
    --add-host="uat:172.31.7.133" \
    --add-host="prod1:172.31.32.34" \
    --add-host="prod2:172.31.30.37" \
    consumer/generic
