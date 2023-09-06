#!/usr/bin/env bash
SCRIPTS_DIR=$(dirname "$0")

GOPATH=$(go env GOPATH)

if [ -z $GOPATH ]; then
    printf "Error: the environment variable GOPATH is not set, please set it before running %s\n" $PROGRAM > /dev/stderr
    exit 1
fi
echo "GOPATH=$GOPATH"

export PATH=$PATH:$GOPATH/bin


# Generate go code
echo "Generating go code..."

proto_dirs=$(find ../protos/bulkload -type d -print)
for dir in $proto_dirs; do
    if [[ "$dir" == "../protos/bulkload" ]]; then
        continue
    fi
    echo "Generating go code for $dir"
    protoc -I=../protos/bulkload --go_out=plugins=grpc,paths=source_relative:../protos/bulkload/ $dir/*.proto
done

