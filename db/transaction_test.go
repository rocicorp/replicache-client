package db

import (
	"testing"

	"github.com/attic-labs/noms/go/nomdl"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/stretchr/testify/assert"
)

func assertDataEquals(assert *assert.Assertions, db *DB, expr string) {
	valueRef := db.head.Value.Data
	expectedValue := nomdl.MustParse(db.Noms(), expr)
	if !valueRef.Equals(types.NewRef(expectedValue)) {
		value := valueRef.TargetValue(db.Noms())
		assert.Fail("Expected %s equal %s", types.EncodeValue(value), expr)
	}
}

func TestDel(t *testing.T) {
	assert := assert.New(t)
	sp, err := spec.ForDatabase("mem")
	assert.NoError(err)
	db, err := Load(sp)
	assert.NoError(err)

	wtx := db.NewTransaction()
	err = wtx.Put("foo", []byte(`"bar"`))
	assert.NoError(err)
	_, err = wtx.Commit()
	assert.NoError(err)

	rtx := db.NewTransaction()
	ok, err := rtx.Has("foo")
	assert.NoError(err)
	assert.True(ok)
	assert.NoError(rtx.Close())

	wtx = db.NewTransaction()
	ok, err = wtx.Del("foo")
	assert.NoError(err)
	assert.True(ok)
	_, err = wtx.Commit()
	assert.NoError(err)

	rtx = db.NewTransaction()
	ok, err = rtx.Has("foo")
	assert.NoError(err)
	assert.False(ok)
	assert.NoError(rtx.Close())

	wtx = db.NewTransaction()
	ok, err = wtx.Del("foo")
	assert.NoError(err)
	assert.False(ok)
	_, err = wtx.Commit()
	assert.NoError(err)
}

func TestReadTransaction(t *testing.T) {
	assert := assert.New(t)
	db, _ := LoadTempDB(assert)

	exp := []byte(`"bar"`)
	wtx := db.NewTransaction()
	err := wtx.Put("foo", exp)
	assert.NoError(err)
	_, err = wtx.Commit()
	assert.NoError(err)

	tx := db.NewTransaction()

	ok, err := tx.Has("foo")
	assert.NoError(err)
	assert.True(ok)
	act, err := tx.Get("foo")
	assert.NoError(err)
	assert.Equal(exp, act, "expected %s got %s", exp, act)

	ok, err = tx.Has("bar")
	assert.NoError(err)
	assert.False(ok)

	act, err = tx.Get("bar")
	assert.NoError(err)
	assert.Nil(act)

	err = tx.Close()
	assert.NoError(err)
}

func TestClosedTransaction(t *testing.T) {
	assert := assert.New(t)
	db, _ := LoadTempDB(assert)

	tx := db.NewTransaction()
	assert.False(tx.Closed())
	_, err := tx.Commit()
	assert.NoError(err)
	assert.True(tx.Closed())

	_, err = tx.Has("k")
	assert.Equal(ErrClosed, err)
	_, err = tx.Get("k")
	assert.Equal(ErrClosed, err)
	_, err = tx.Scan(ScanOptions{})
	assert.Equal(ErrClosed, err)
	err = tx.Put("k", []byte(`"v"`))
	assert.Equal(ErrClosed, err)
	_, err = tx.Del("k")
	assert.Equal(ErrClosed, err)
	_, err = tx.Commit()
	assert.Equal(ErrClosed, err)
	err = tx.Close()
	assert.Equal(ErrClosed, err)
}

func TestWriteTransaction(t *testing.T) {
	assert := assert.New(t)
	db, _ := LoadTempDB(assert)

	exp := []byte(`"bar"`)
	tx := db.NewTransaction()

	err := tx.Put("foo", exp)
	assert.NoError(err)

	ok, err := tx.Has("foo")
	assert.NoError(err)
	assert.True(ok)
	act, err := tx.Get("foo")
	assert.NoError(err)
	assert.Equal(exp, act, "expected %s got %s", exp, act)

	ok, err = tx.Has("bar")
	assert.NoError(err)
	assert.False(ok)

	act, err = tx.Get("bar")
	assert.NoError(err)
	assert.Nil(act)

	assertDataEquals(assert, db, `map {}`)
	_, err = tx.Commit()
	assert.NoError(err)
	assertDataEquals(assert, db, `map {"foo": "bar"}`)
}

func TestReadAndWriteTransaction(t *testing.T) {
	assert := assert.New(t)
	db, _ := LoadTempDB(assert)

	exp := []byte(`"bar"`)
	rtx := db.NewTransaction()

	wtx := db.NewTransaction()
	err := wtx.Put("foo", exp)
	assert.NoError(err)

	has, err := wtx.Has("foo")
	assert.NoError(err)
	assert.True(has)

	has, err = rtx.Has("foo")
	assert.NoError(err)
	assert.False(has)

	act, err := wtx.Get("foo")
	assert.NoError(err)
	assert.Equal(exp, act, "expected %s got %s", exp, act)

	act, err = rtx.Get("foo")
	assert.NoError(err)
	assert.Nil(act)

	assertDataEquals(assert, db, `map {}`)

	_, err = wtx.Commit()
	assert.NoError(err)

	has, err = rtx.Has("foo")
	assert.NoError(err)
	assert.False(has, "Read transaction should still operate at old head")

	err = rtx.Close()
	assert.NoError(err)
}

func TestMultipleWriteTransaction(t *testing.T) {
	assert := assert.New(t)
	db, _ := LoadTempDB(assert)

	v1 := []byte(`"v1"`)
	v2 := []byte(`"v2"`)

	tx1 := db.NewTransaction()
	err := tx1.Put("k", v1)
	assert.NoError(err)

	tx2 := db.NewTransaction()
	err = tx2.Put("k", v2)
	assert.NoError(err)

	act, err := tx1.Get("k")
	assert.NoError(err)
	assert.Equal(v1, act, "expected %s got %s", v1, act)

	act, err = tx2.Get("k")
	assert.NoError(err)
	assert.Equal(v2, act, "expected %s got %s", v2, act)

	_, err = tx1.Commit()
	assert.NoError(err)
	assertDataEquals(assert, db, `map {"k": "v1"}`)

	_, err = tx2.Commit()
	assert.Equal("Dataset head is not ancestor of commit", err.Error())
	assertDataEquals(assert, db, `map {"k": "v1"}`)
}

func TestMultipleWriteTransactionClose(t *testing.T) {
	assert := assert.New(t)
	db, _ := LoadTempDB(assert)

	v1 := []byte(`"v1"`)
	v2 := []byte(`"v2"`)

	tx1 := db.NewTransaction()
	err := tx1.Put("k", v1)
	assert.NoError(err)

	tx2 := db.NewTransaction()
	err = tx2.Put("k", v2)
	assert.NoError(err)

	act, err := tx1.Get("k")
	assert.NoError(err)
	assert.Equal(v1, act, "expected %s got %s", v1, act)

	act, err = tx2.Get("k")
	assert.NoError(err)
	assert.Equal(v2, act, "expected %s got %s", v2, act)

	err = tx1.Close()
	assert.NoError(err)
	assertDataEquals(assert, db, `map {}`)

	_, err = tx2.Commit()
	assert.NoError(err)
	assertDataEquals(assert, db, `map {"k": "v2"}`)
}
