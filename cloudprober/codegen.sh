#!/usr/bin/env bash
cd "$(dirname "$0")"

rm -rf google

for p in $(find ../third_party/googleapis/google -type f -name *.proto); do
	protoc \
    --proto_path=../third_party/googleapis \
    --csharp_out=./ \
    --grpc_out=./output \
    --plugin=protoc-gen-grpc=./bins/opt/grpc_csharp_plugin \
    "$p"
done
