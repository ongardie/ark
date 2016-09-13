#!/bin/bash

set -ex
trap 'kill -HUP 0' EXIT

go build

if [ "x$1" == "xclean" ]; then
  rm -rf store/ logs/
  mkdir -p logs
  ./zoolater -id 1 -clientaddr 127.0.0.1:2181 -peeraddr 127.0.0.1:2191 -adminaddr 127.0.0.1:2171 -bootstrap >logs/server1 2>&1
fi
mkdir -p logs

./zoolater -id 1 -clientaddr 127.0.0.1:2181 -peeraddr 127.0.0.1:2191 -adminaddr 127.0.0.1:2171 >>logs/server1 2>&1 &
./zoolater -id 2 -clientaddr 127.0.0.1:2182 -peeraddr 127.0.0.1:2192 -adminaddr 127.0.0.1:2172 >>logs/server2 2>&1 &
./zoolater -id 3 -clientaddr 127.0.0.1:2183 -peeraddr 127.0.0.1:2193 -adminaddr 127.0.0.1:2173 >>logs/server3 2>&1 &

sleep 3
for data in '{"ServerId": "server2", "Address": "127.0.0.1:2192"}' '{"ServerId": "server3", "Address": "127.0.0.1:2193"}'; do
  curl -H 'Content-Type: application/json' --data "$data" -w '\nSTATUS %{http_code}\n' 127.0.0.1:2171/api/raft/addvoter
done

wait
wait
wait

