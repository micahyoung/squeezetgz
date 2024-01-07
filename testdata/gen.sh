#!/bin/bash
set -o errexit -o pipefail -o nounset

cd "$(dirname "${0}")"

dict="/usr/share/dict/words"
dictsize=$(wc -l < ${dict})
for i in {2,4,5,6,7,8,10,20,100,1000}; do
    mkdir -p "${i}"
    
    split -l$((dictsize/i)) -a 3 $dict "${i}/"
done

docker rm -f registry || true
docker run --name registry -d -p 6000:5000 registry:2

for image in alpine:20231219 ubuntu:noble-20231221 node:21.5.0-alpine3.19 python:3.9.18-bookworm; do
    read -r name tag <<< "${image//:/ }"
    if ! [ -f ${name}.tar.gz ]; then
        crane flatten ${image} --tag=localhost:6000/${name}:flat --platform=linux/amd64
        crane pull localhost:6000/${name}:flat /tmp/image --platform=linux/amd64 
        layertgz=$(tar xOf /tmp/image manifest.json | jq -r '.[].Layers[0]')
        tar xOf /tmp/image $layertgz > ${name}.tar.gz
        rm /tmp/image
    fi
done

docker rm -f registry || true
