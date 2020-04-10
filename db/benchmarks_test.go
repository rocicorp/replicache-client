package db

import (
	"fmt"
	"testing"

	"github.com/attic-labs/noms/go/types"
	"github.com/stretchr/testify/assert"
)

func BenchmarkPut(b *testing.B) {
	assert := assert.New(b)
	db, dir := LoadTempDB(assert)
	fmt.Println(dir)
	for n := 0; n < b.N; n++ {
		err := db.Put("foo", []byte(fmt.Sprintf("%f", types.Number(n))))
		assert.NoError(err)
	}
}
