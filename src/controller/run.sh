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

trap "replace_files_with_sym" 0

docker build -t controller-image . && 
    docker run \
      -it --rm \
      --name controller-container \
      -v $(pwd)/test/monitor_sequence/results:/usr/src/data \
      controller-image
