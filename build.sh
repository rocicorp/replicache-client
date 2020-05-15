# repm
echo "Building repm..."

set -x
ORIG=`pwd`
ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
REPM_VERSION=`git rev-parse HEAD`

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
gomobile bind -ldflags="-s -w -X github.com/diff-server/util/version.v=$REPM_VERSION" --target=ios ../repm/
gomobile bind -ldflags="-s -w -X github.com/diff-server/util/version.v=$REPM_VERSION" --target=android ../repm/

export GO111MODULE=

cd $ORIG
