#!/bin/sh

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
ROOT=$DIR/../
set -x

rm -rf build
mkdir build
cd build

# repl
echo "Building repl..."

REPL_VERSION=`git describe --tags`
cd $ROOT
GOOS=darwin GOARCH=amd64 go build -ldflags "-X roci.dev/diff-server/util/version.v=$REPL_VERSION" -o build/repl-darwin-amd64 ./cmd/repl
GOOS=linux GOARCH=amd64 go build -ldflags "-X roci.dev/diff-server/util/version.v=$REPL_VERSION" -o build/repl-linux-amd64 ./cmd/repl
