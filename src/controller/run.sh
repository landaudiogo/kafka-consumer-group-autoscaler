#!/bin/bash

set -eo nounset

IFS=$'\n' arrayRealFiles=(
  $(find . -type l -print | xargs -d '\n' -I @ realpath @)
)

IFS=$'\n' arraySymFiles=(
  $(find . -type l -print)
)

for file in "${arraySymFiles[@]}"; do
  rm "$file"
done

for file in "${arrayRealFiles[@]}"; do
  cp "$file" ./
done

replace_files_with_sym() {
  for file in "${arrayRealFiles[@]}"; do
    rm "$(basename $file)"
    ln -s "$file" ./
  done
}

docker build -t controller-image . && 
  { 
    docker run \
      -it --rm \
      --network kafka_kafka-network \
      --name controller-container \
      --add-host="ip-172-31-7-133.eu-west-1.compute.internal:172.31.7.133" \
      --add-host="ip-172-31-32-34.eu-west-1.compute.internal:172.31.32.34" \
      --add-host="ip-172-31-30-37.eu-west-1.compute.internal:172.31.30.37" \
      --add-host="uat:172.31.7.133" \
      --add-host="prod1:172.31.32.34" \
      --add-host="prod2:172.31.30.37" \
      -v controller-volume:/usr/src/data \
      controller-image || { replace_files_with_sym; exit 1; }
  }

replace_files_with_sym
