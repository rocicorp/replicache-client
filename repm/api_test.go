package repm

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
	gtime "time"

	"github.com/attic-labs/noms/go/spec"
	"github.com/stretchr/testify/assert"

	jsnoms "roci.dev/diff-server/util/noms/json"
	"roci.dev/diff-server/util/time"
)

func TestBasics(t *testing.T) {
	defer deinit()
	defer time.SetFake()()

	assert := assert.New(t)
	dir, err := ioutil.TempDir("", "")
	Init(dir, "", nil)
	ret, err := Dispatch("db1", "open", nil)
	assert.Nil(ret)
	assert.NoError(err)

	const invalidRequest = ""
	const invalidRequestError = "unexpected end of JSON input"

	tc := []struct {
		rpc              string
		req              string
		expectedResponse string
		expectedError    string
	}{
		// invalid json for all cases
		// valid json + success case for all cases
		// valid json + failure case for all cases
		// attempt to write non-json with put()
		// attempt to read non-json with get()

		// getRoot on empty db
		{"getRoot", `{}`, `{"root":"4p3l8m7gjkkd8g3g0glothm038s61123"}`, ""},

		// put
		{"put", invalidRequest, ``, invalidRequestError},
		{"getRoot", `{}`, `{"root":"4p3l8m7gjkkd8g3g0glothm038s61123"}`, ""}, // getRoot when db didn't change
		{"put", `{"id": "foo"}`, ``, "value field is required"},
		{"put", `{"id": "foo", "value": null}`, ``, "value field is required"},
		{"put", `{"id": "foo", "value": "bar"}`, `{"root":"0msppp2die542he6b4udelpe165gh1i2"}`, ""},
		{"getRoot", `{}`, `{"root":"0msppp2die542he6b4udelpe165gh1i2"}`, ""}, // getRoot when db did change

		// has
		{"has", invalidRequest, ``, invalidRequestError},
		{"has", `{"id": "foo"}`, `{"has":true}`, ""},

		// get
		{"get", invalidRequest, ``, invalidRequestError},
		{"get", `{"id": "foo"}`, `{"has":true,"value":"bar"}`, ""},

		// scan
		{"put", `{"id": "foopa", "value": "doopa"}`, `{"root":"q05io6idqml885dv4paq3eb1vc6ndo3s"}`, ""},
		{"scan", `{"prefix": "foo"}`, `[{"id":"foo","value":"bar"},{"id":"foopa","value":"doopa"}]`, ""},
		{"scan", `{"start": {"id": {"value": "foo"}}}`, `[{"id":"foo","value":"bar"},{"id":"foopa","value":"doopa"}]`, ""},
		{"scan", `{"start": {"id": {"value": "foo", "exclusive": true}}}`, `[{"id":"foopa","value":"doopa"}]`, ""},

		// TODO: other scan operators
	}

	for _, t := range tc {
		res, err := Dispatch("db1", t.rpc, []byte(t.req))
		if t.expectedError != "" {
			assert.Nil(res, "test case %s: %s", t.rpc, t.req, "test case %s: %s", t.rpc, t.req)
			assert.EqualError(err, t.expectedError, "test case %s: %s", t.rpc, t.req)
		} else {
			assert.Equal(t.expectedResponse, string(res), "test case %s: %s", t.rpc, t.req)
			assert.NoError(err, "test case %s: %s", t.rpc, t.req, "test case %s: %s", t.rpc, t.req)
		}
	}
}

func TestProgress(t *testing.T) {
	assert := assert.New(t)
	dir, err := ioutil.TempDir("", "")
	fmt.Println("dir", dir)
	Init(dir, "", nil)
	ret, err := Dispatch("db1", "open", nil)
	assert.Nil(ret)
	assert.NoError(err)

	twoChunks := [][]byte{[]byte(`"foo`), []byte(`bar"`)}

	getProgress := func() (received, expected uint64) {
		buf, err := Dispatch("db1", "syncProgress", mustMarshal(SyncProgressRequest{}))
		assert.NoError(err)
		var resp SyncProgressResponse
		err = json.Unmarshal(buf, &resp)
		assert.NoError(err)
		return resp.BytesReceived, resp.BytesExpected
	}

	totalLength := uint64(len(twoChunks[0]) + len(twoChunks[1]))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-length", fmt.Sprintf("%d", totalLength))
		seen := uint64(0)
		rec, exp := getProgress()
		assert.Equal(uint64(0), rec)
		assert.Equal(uint64(0), exp)
		for _, c := range twoChunks {
			seen += uint64(len(c))
			_, err := w.Write(c)
			assert.NoError(err)
			w.(http.Flusher).Flush()
			gtime.Sleep(100 * gtime.Millisecond)
			rec, exp := getProgress()
			assert.Equal(seen, rec)
			assert.Equal(totalLength, exp)
		}
	}))

	sp, err := spec.ForDatabase(server.URL)
	assert.NoError(err)
	req := SyncRequest{
		Remote:  jsnoms.Spec{sp},
	}

	_, err = Dispatch("db1", "requestSync", mustMarshal(req))
	assert.Regexp(`is not valid JSON`, err.Error())
}
