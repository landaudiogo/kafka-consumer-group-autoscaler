#!/bin/bash
repository="consumer-capacity"
remoteVersions=($(wget -q https://registry.hub.docker.com/v1/repositories/thehuub/$repository/tags -O -  | sed -e 's/[][]//g' -e 's/"//g' -e 's/ //g' | tr '}' '\n'  | awk -F: '{print $3}'))

read -p "Dockerfile directory: " directory
read -p "Choose version (latest is ${remoteVersions[-1]}): " version
imageName="thehuub/$repository:${version}"

read -p "Tag image with $imageName? [Y|n] " opt
[ "$opt" = "n" ] && {
  printf "Exiting script\n"
  exit 0
}

docker build --no-cache "$directory" -t "$imageName" \
  || {
    printf "\n\nFailed to build image\n"
    printf "verify Dockerfile directory or build output\n"
    exit 1
  }
docker push "$imageName" \
  || { 
    printf "\n\nFailed to push the image to the repository\n"
    printf "Verify docker account\n"
    exit 1
  }

