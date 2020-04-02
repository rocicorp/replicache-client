# repm
echo "Building repm..."

set -x
ORIG=`pwd`
ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
REPL_VERSION=`git describe --tags`

cd $ROOT
rm -rf vendor
go mod vendor > /dev/null 2>&1
cd $GOPATH/src

# Need to turn this off to build repm because Gomobile doesn't support modules,
# and as of go 1.13 the default is on if the source code contains a go.mod file,
# regardless of location.
export GO111MODULE=off

mkdir -p roci.dev
rm roci.dev/replicache-client
ln -s $ROOT roci.dev/replicache-client > /dev/null 2>&1 
cd roci.dev/replicache-client
rm -rf build
mkdir build
cd build
gomobile bind -ldflags="-s -w -X github.com/diff-server/util/version.v=$REPL_VERSION" --target=ios ../repm/
gomobile bind -ldflags="-s -w -X github.com/diff-server/util/version.v=$REPL_VERSION" --target=android ../repm/
tar -czvf Repm.framework.tar.gz Repm.framework

export GO111MODULE=

# repl
echo "Building repl..."

REPL_VERSION=`git describe --tags`
cd $ROOT
GOOS=darwin GOARCH=amd64 go build -ldflags "-X roci.dev/diff-server/util/version.v=$REPL_VERSION" -o build/repl-darwin-amd64 ./cmd/repl
GOOS=linux GOARCH=amd64 go build -ldflags "-X roci.dev/diff-server/util/version.v=$REPL_VERSION" -o build/repl-linux-amd64 ./cmd/repl

# noms tool
echo "Building noms..."
NOMS_VERSION=`go mod graph | grep '^github.com/attic-labs/noms@' | cut -d' ' -f1 | head -n1`
go get $NOMS_VERSION
GOOS=darwin GOARCH=amd64 go build -o build/noms-darwin-amd64 github.com/attic-labs/noms/cmd/noms
GOOS=linux GOARCH=amd64 go build -o build/noms-linux-amd64 github.com/attic-labs/noms/cmd/noms

cd $ORIG
