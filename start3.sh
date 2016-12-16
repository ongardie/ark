#!/bin/bash

set -ex
trap 'kill -HUP 0' EXIT

go build

if [ "x$1" == "xclean" ]; then
  rm -rf store/ logs/
  mkdir -p logs
  openssl req -x509 -sha256 -nodes -days 1 -newkey rsa:2048 -keyout key.key -out cert.crt -batch
  ./ark -id server1 -peer-address 127.0.0.1:2191 -bootstrap >logs/server1 2>&1
fi
mkdir -p logs

./ark -id server1 -peer-address 127.0.0.1:2191 -cleartext-client-address 127.0.0.1:2181 -tls-client-address 127.0.0.1:2281 -admin-address 127.0.0.1:2171 >>logs/server1 2>&1 &
./ark -id server2 -peer-address 127.0.0.1:2192 -cleartext-client-address 127.0.0.1:2182 -tls-client-address 127.0.0.1:2282 -admin-address 127.0.0.1:2172 >>logs/server2 2>&1 &
./ark -id server3 -peer-address 127.0.0.1:2193 -cleartext-client-address 127.0.0.1:2183 -tls-client-address 127.0.0.1:2283 -admin-address 127.0.0.1:2173 >>logs/server3 2>&1 &

sleep 3
for data in '{"ServerId": "server2", "Address": "127.0.0.1:2192"}' '{"ServerId": "server3", "Address": "127.0.0.1:2193"}'; do
  curl -H 'Content-Type: application/json' --data "$data" -w '\nSTATUS %{http_code}\n' 127.0.0.1:2171/api/raft/addvoter
done

wait
wait
wait

