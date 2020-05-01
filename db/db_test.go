package db

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/attic-labs/noms/go/spec"
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

	db, dir := LoadTempDB(assert)

	tx := db.NewTransaction()
	assert.False(tx.Has("foo"))
	v, err := tx.Get("foo")
	assert.Nil(v)
	assert.NoError(err)
	m := kv.NewMap(db.noms)
	assert.True(db.head.NomsStruct.Equals(makeGenesis(db.noms, "", db.noms.WriteValue(m.NomsMap()), m.NomsChecksum(), 0).NomsStruct))

	cid := db.clientID
	assert.NotEqual("", cid)

	db = reloadDB(assert, dir)
	assert.Equal(cid, db.clientID)
	err = tx.Close()
	assert.NoError(err)
}

func TestData(t *testing.T) {
	assert := assert.New(t)
	db, dir := LoadTempDB(assert)

	exp := []byte(`"bar"`)
	tx := db.NewTransaction()
	err := tx.Put("foo", exp)
	assert.NoError(err)
	_, err = tx.Commit()
	assert.NoError(err)

	dbs := []*DB{
		db, reloadDB(assert, dir),
	}

	for _, d := range dbs {
		tx := d.NewTransaction()
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
