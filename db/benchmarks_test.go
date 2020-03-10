package db

import (
	"fmt"
	"testing"

	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/stretchr/testify/assert"
)

func BenchmarkPut(b *testing.B) {
	assert := assert.New(b)
	db, dir := LoadTempDB(assert)
	fmt.Println(dir)
	for n := 0; n < b.N; n++ {
		err := db.Put("foo", types.Number(n))
		assert.NoError(err)
	}
}

func BenchmarkExecHTTP(b *testing.B) {
	benchmarkExec(true, b)
}

func BenchmarkExecLocal(b *testing.B) {
	benchmarkExec(false, b)
}

func benchmarkExec(http bool, b *testing.B) {
	assert := assert.New(b)
	var db *DB
	if http {
		sp, err := spec.ForDatabase("https://serve.replicate.to/sandbox/benchmark-test")
		assert.NoError(err)
		db, err = Load(sp)
		assert.NoError(err)
	} else {
		var dir string
		db, dir = LoadTempDB(assert)
		fmt.Println(dir)
	}

	//db.PutBundle([]byte("function put(k, v) { db.put(k, v); }"))

	for n := 0; n < b.N; n++ {
		_, err := db.Exec("put", types.NewList(db.Noms(), types.String("foo"), types.Number(n)))
		assert.NoError(err)
	}
}
