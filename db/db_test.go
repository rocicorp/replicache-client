package db

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/stretchr/testify/assert"
	"roci.dev/diff-server/kv"
)

func reloadDB(assert *assert.Assertions, dir string) (db *DB) {
	sp, err := spec.ForDatabase(dir)
	assert.NoError(err)

	db, err = Load(sp)
	assert.NoError(err)

	return db
}

func TestGenesis(t *testing.T) {
	assert := assert.New(t)

	db, _ := LoadTempDB(assert)

	assert.False(db.Has("foo"))
	v, err := db.Get("foo")
	assert.Nil(v)
	assert.NoError(err)
	m := kv.NewMap(db.noms)
	assert.True(db.head.Original.Equals(makeGenesis(db.noms, "", db.noms.WriteValue(m.NomsMap()), types.String(m.Checksum().String())).Original))
}

func TestData(t *testing.T) {
	assert := assert.New(t)
	db, dir := LoadTempDB(assert)

	exp := types.String("bar")
	err := db.Put("foo", exp)
	assert.NoError(err)

	dbs := []*DB{
		db, reloadDB(assert, dir),
	}

	for _, d := range dbs {
		ok, err := d.Has("foo")
		assert.NoError(err)
		assert.True(ok)
		act, err := d.Get("foo")
		assert.NoError(err)
		assert.True(act.Equals(exp), "expected %s got %s", exp, act)

		ok, err = d.Has("bar")
		assert.NoError(err)
		assert.False(ok)

		act, err = d.Get("bar")
		assert.NoError(err)
		assert.Nil(act)
	}
}

func TestDel(t *testing.T) {
	assert := assert.New(t)
	sp, err := spec.ForDatabase("mem")
	assert.NoError(err)
	db, err := Load(sp)
	assert.NoError(err)

	err = db.Put("foo", types.String("bar"))
	assert.NoError(err)

	ok, err := db.Has("foo")
	assert.NoError(err)
	assert.True(ok)

	ok, err = db.Del("foo")
	assert.NoError(err)
	assert.True(ok)

	ok, err = db.Has("foo")
	assert.NoError(err)
	assert.False(ok)

	ok, err = db.Del("foo")
	assert.NoError(err)
	assert.False(ok)
}

func TestLoadBadSpec(t *testing.T) {
	assert := assert.New(t)

	sp, err := spec.ForDatabase("http://localhost:6666") // not running, presumably
	assert.NoError(err)
	db, err := Load(sp)
	assert.Nil(db)
	assert.Regexp(`Get "?http://localhost:6666/root/"?: dial tcp (.+?):6666: connect: connection refused`, err.Error())

	srv := httptest.NewServer(http.NotFoundHandler())
	sp, err = spec.ForDatabase(srv.URL)
	assert.NoError(err)
	db, err = Load(sp)
	assert.Nil(db)
	assert.EqualError(err, "Unexpected response: Not Found: 404 page not found")
}
