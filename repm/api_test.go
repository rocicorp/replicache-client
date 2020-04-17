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
		{"put", `{"id": "foo", "value": "bar"}`, "", "Missing transaction ID"},

		{"openTransaction", `{}`, `{"transactionId":1}`, ""},
		{"put", `{"transactionId": 1, "id": "foo", "value": "bar"}`, `{}`, ""},
		{"put", `{"transactionId": 1, "id": "foo"}`, ``, "value field is required"},
		{"put", `{"transactionId": 1, "id": "foo", "value": null}`, `{}`, ""},
		{"put", `{"transactionId": 1, "id": "foo", "value": "bar"}`, `{}`, ""}, // so we can scan it
		{"getRoot", `{}`, `{"root":"4p3l8m7gjkkd8g3g0glothm038s61123"}`, ""},   // getRoot when db did change
		{"commitTransaction", `{"transactionId":1}`, `{"ref":"p34f8g8jghkainifnsp966oqgf3pv88t"}`, ""},
		{"getRoot", `{}`, `{"root":"d5024qks1v8sk57tjfg7ml14nugdm8e1"}`, ""}, // getRoot when db did change

		// has

		{"has", invalidRequest, ``, invalidRequestError},
		{"has", `{"id": "foo"}`, ``, "Missing transaction ID"},
		{"openTransaction", `{}`, `{"transactionId":2}`, ""},
		{"has", `{"transactionId": 2, "id": "foo"}`, `{"has":true}`, ""},
		{"closeTransaction", `{"transactionId": 2}`, `{}`, ""},

		// get
		{"get", invalidRequest, ``, invalidRequestError},
		{"get", `{"id": "foo"}`, "", "Missing transaction ID"},
		{"openTransaction", `{}`, `{"transactionId":3}`, ""},
		{"get", `{"transactionId": 3, "id": "foo"}`, `{"has":true,"value":"bar"}`, ""},
		{"closeTransaction", `{"transactionId": 3}`, `{}`, ""},

		// scan
		{"openTransaction", `{}`, `{"transactionId":4}`, ""},
		{"put", `{"transactionId": 4, "id": "foopa", "value": "doopa"}`, `{}`, ""},
		{"commitTransaction", `{"transactionId":4}`, `{"ref":"n5sg13l13g5odv9kla1r5r5k7lhlef74"}`, ""},
		{"openTransaction", `{}`, `{"transactionId":5}`, ""},
		{"scan", `{"transactionId": 5, "prefix": "foo"}`, `[{"id":"foo","value":"bar"},{"id":"foopa","value":"doopa"}]`, ""},
		{"scan", `{"transactionId": 5, "start": {"id": {"value": "foo"}}}`, `[{"id":"foo","value":"bar"},{"id":"foopa","value":"doopa"}]`, ""},
		{"scan", `{"transactionId": 5, "start": {"id": {"value": "foo", "exclusive": true}}}`, `[{"id":"foopa","value":"doopa"}]`, ""},
		{"closeTransaction", `{"transactionId":5}`, `{}`, ""},

		// TODO: other scan operators
	}

	for _, t := range tc {
		res, err := Dispatch("db1", t.rpc, []byte(t.req))
		if t.expectedError != "" {
			assert.Nil(res, "test case %s: %s", t.rpc, t.req, "test case %s: %s", t.rpc, t.req)
			assert.EqualError(err, t.expectedError, "test case %s: %s", t.rpc, t.req)
		} else {
			assert.Equal(t.expectedResponse, string(res), "test case %s: %s", t.rpc, t.req)
			assert.NoError(err, "test case %s: %s", t.rpc, t.req)
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
		buf, err := Dispatch("db1", "pullProgress", mustMarshal(PullProgressRequest{}))
		assert.NoError(err)
		var resp PullProgressResponse
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
	req := PullRequest{
		Remote: jsnoms.Spec{sp},
	}

	_, err = Dispatch("db1", "pull", mustMarshal(req))
	assert.Regexp(`is not valid JSON`, err.Error())
}
