package repm

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"roci.dev/diff-server/util/time"
	"roci.dev/diff-server/util/version"
)

func mm(assert *assert.Assertions, in interface{}) []byte {
	r, err := json.Marshal(in)
	assert.NoError(err)
	return r
}

func TestLog(t *testing.T) {
	defer deinit()
	defer time.SetFake()()

	assert := assert.New(t)
	dir, err := ioutil.TempDir("", "")
	assert.NoError(err)
	buf := &bytes.Buffer{}
	Init(dir, "", buf)
	Dispatch("db1", "open", nil)

	assert.Regexp(`^GR[0-9a-f]{9} \d{4}\/\d{2}\/\d{2} \d{2}:\d{2}:\d{2}\.\d+`, string(buf.Bytes()))
}

func s(b []byte) string {
	return strings.TrimRight(string(b), "\n")
}

func TestBasic(t *testing.T) {
	defer deinit()
	defer time.SetFake()()

	assert := assert.New(t)
	dir, err := ioutil.TempDir("", "")
	Init(dir, "", nil)

	res, err := Dispatch("db1", "open", nil)
	assert.Nil(res)
	assert.NoError(err)

	{
		resp, err := Dispatch("db1", "openTransaction", []byte(`{}`))
		assert.NoError(err)
		assert.Equal(`{"transactionId":1}`, s(resp))

		resp, err = Dispatch("db1", "put", []byte(`{"transactionId": 1, "id": "foo", "value": "bar"}`))
		assert.Equal(`{}`, s(resp))
		assert.NoError(err)

		resp, err = Dispatch("db1", "commitTransaction", []byte(`{"transactionId": 1}`))
		assert.Equal(`{"ref":"p34f8g8jghkainifnsp966oqgf3pv88t"}`, s(resp))
		assert.NoError(err)
	}

	{
		resp, err := Dispatch("db1", "openTransaction", []byte(`{}`))
		assert.NoError(err)
		assert.Equal(`{"transactionId":2}`, s(resp))

		resp, err = Dispatch("db1", "get", []byte(`{"transactionId": 2, "id": "foo"}`))
		assert.Equal(`{"has":true,"value":"bar"}`, string(resp))

		resp, err = Dispatch("db1", "closeTransaction", []byte(`{"transactionId": 2}`))
		assert.Equal(`{}`, s(resp))
		assert.NoError(err)
	}

	{
		resp, err := Dispatch("db1", "openTransaction", []byte(`{}`))
		assert.NoError(err)
		assert.Equal(`{"transactionId":3}`, s(resp))

		resp, err = Dispatch("db1", "del", []byte(`{"transactionId": 3, "id": "foo"}`))
		assert.Equal(`{"ok":true}`, s(resp))

		resp, err = Dispatch("db1", "commitTransaction", []byte(`{"transactionId": 3}`))
		assert.Equal(`{"ref":"itlkmsge0msn3pj3upcpgh17rpsvil83"}`, s(resp))
		assert.NoError(err)
	}

	testFile, err := ioutil.TempFile(connections["db1"].dir, "")
	assert.NoError(err)

	{
		resp, err := Dispatch("db1", "openTransaction", []byte(`{}`))
		assert.NoError(err)
		assert.Equal(`{"transactionId":4}`, s(resp))

		resp, err = Dispatch("db2", "put", []byte(`{"transactionId": 4, "id": "foo", "value": "bar"}`))
		assert.Nil(resp)
		assert.EqualError(err, "specified database is not open")
	}

	resp, err := Dispatch("db1", "close", nil)
	assert.Nil(resp)
	assert.NoError(err)

	resp, err = Dispatch("db1", "openTransaction", []byte(`{}`))
	assert.Nil(resp)
	assert.EqualError(err, "specified database is not open")

	resp, err = Dispatch("db1", "drop", nil)
	assert.Nil(resp)
	assert.NoError(err)
	fi, err := os.Stat(testFile.Name())
	assert.Equal(nil, fi)
	assert.True(os.IsNotExist(err))

	resp, err = Dispatch("", "version", nil)
	assert.Equal(version.Version(), string(resp))
}

func TestList(t *testing.T) {
	defer deinit()
	assert := assert.New(t)

	repDir = ""

	rb, err := Dispatch("", "list", nil)
	assert.EqualError(err, "must call init first")
	assert.Nil(rb)

	Init("/not/existent/dir", "", nil)
	rb, err = Dispatch("", "list", nil)
	assert.NoError(err)
	assert.Equal(`{"databases":[]}`, string(rb))

	dir, err := ioutil.TempDir("", "")
	assert.NoError(err)

	Init(dir, "", nil)
	rb, err = Dispatch("", "list", nil)
	assert.NoError(err)
	assert.Equal(`{"databases":[]}`, string(rb))

	rb, err = Dispatch("db1", "open", nil)
	assert.Nil(rb)
	assert.NoError(err)

	rb, err = Dispatch("", "list", nil)
	assert.NoError(err)
	assert.Equal(`{"databases":[{"name":"db1"}]}`, string(rb))

	rb, err = Dispatch("db1", "open", nil)
	assert.Nil(rb)
	assert.NoError(err)

	rb, err = Dispatch("", "list", nil)
	assert.NoError(err)
	assert.Equal(`{"databases":[{"name":"db1"}]}`, string(rb))

	rb, err = Dispatch("db2", "open", nil)
	assert.Nil(rb)
	assert.NoError(err)

	rb, err = Dispatch("", "list", nil)
	assert.NoError(err)
	assert.Equal(`{"databases":[{"name":"db1"},{"name":"db2"}]}`, string(rb))

	rb, err = Dispatch("db1", "drop", nil)
	assert.Nil(rb)
	assert.NoError(err)

	rb, err = Dispatch("", "list", nil)
	assert.NoError(err)
	assert.Equal(`{"databases":[{"name":"db2"}]}`, string(rb))

	rb, err = Dispatch("db2", "drop", nil)
	assert.Nil(rb)
	assert.NoError(err)

	rb, err = Dispatch("", "list", nil)
	assert.NoError(err)
	assert.Equal(`{"databases":[]}`, string(rb))

	err = ioutil.WriteFile(path.Join(dir, "file.txt"), []byte("foo"), 0644)
	assert.NoError(err)

	rb, err = Dispatch("", "list", nil)
	assert.NoError(err)
	assert.Equal(`{"databases":[]}`, string(rb))

	err = os.Mkdir(path.Join(dir, "-not-valid-base64"), 0755)
	assert.NoError(err)

	rb, err = Dispatch("", "list", nil)
	assert.NoError(err)
	assert.Equal(`{"databases":[]}`, string(rb))

	rb, err = Dispatch("db1", "open", nil)
	assert.Nil(rb)
	assert.NoError(err)

	// Should still return valid databases, skipping over other garbage directory entries.
	rb, err = Dispatch("", "list", nil)
	assert.NoError(err)
	assert.Equal(`{"databases":[{"name":"db1"}]}`, string(rb))
}
