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
		{"getRoot", `{}`, `{"root":"e99uif9c7bpavajrt666es1ki52dv239"}`, ""},

		// put
		{"put", invalidRequest, ``, invalidRequestError},
		{"getRoot", `{}`, `{"root":"e99uif9c7bpavajrt666es1ki52dv239"}`, ""}, // getRoot when db didn't change
		{"put", `{"key": "foo", "value": "bar"}`, "", "Missing transaction ID"},

		{"openTransaction", `{}`, `{"transactionId":1}`, ""},
		{"put", `{"transactionId": 1, "key": "foo", "value": "bar"}`, `{}`, ""},
		{"put", `{"transactionId": 1, "key": "foo"}`, ``, "value field is required"},
		{"put", `{"transactionId": 1, "key": "foo", "value": null}`, `{}`, ""},
		{"put", `{"transactionId": 1, "key": "foo", "value": "bar"}`, `{}`, ""}, // so we can scan it
		{"getRoot", `{}`, `{"root":"e99uif9c7bpavajrt666es1ki52dv239"}`, ""},    // getRoot when db did change
		{"commitTransaction", `{"transactionId":1}`, `{"ref":"eft96l0n1os3dbmjga6n59pblc00roj9"}`, ""},
		{"getRoot", `{}`, `{"root":"eft96l0n1os3dbmjga6n59pblc00roj9"}`, ""}, // getRoot when db did change

		// has
		{"has", invalidRequest, ``, invalidRequestError},
		{"has", `{"key": "foo"}`, ``, "Missing transaction ID"},
		{"openTransaction", `{}`, `{"transactionId":2}`, ""},
		{"has", `{"transactionId": 2, "key": "foo"}`, `{"has":true}`, ""},
		{"closeTransaction", `{"transactionId": 2}`, `{}`, ""},

		// get
		{"get", invalidRequest, ``, invalidRequestError},
		{"get", `{"key": "foo"}`, "", "Missing transaction ID"},
		{"openTransaction", `{}`, `{"transactionId":3}`, ""},
		{"get", `{"transactionId": 3, "key": "foo"}`, `{"has":true,"value":"bar"}`, ""},
		{"closeTransaction", `{"transactionId": 3}`, `{}`, ""},

		// scan
		{"openTransaction", `{"name": "foo", "args": []}`, `{"transactionId":4}`, ""},
		{"put", `{"transactionId": 4, "key": "foopa", "value": "doopa"}`, `{}`, ""},
		{"commitTransaction", `{"transactionId":4}`, `{"ref":"ir8von08b95s17t04gjk6qjas4m7om78"}`, ""},
		{"openTransaction", `{}`, `{"transactionId":5}`, ""},
		{"scan", `{"transactionId": 5, "prefix": "foo"}`, `[{"key":"foo","value":"bar"},{"key":"foopa","value":"doopa"}]`, ""},
		{"scan", `{"transactionId": 5, "start": {"id": {"value": "foo"}}}`, `[{"key":"foo","value":"bar"},{"key":"foopa","value":"doopa"}]`, ""},
		{"scan", `{"transactionId": 5, "start": {"id": {"value": "foo", "exclusive": true}}}`, `[{"key":"foopa","value":"doopa"}]`, ""},
		{"closeTransaction", `{"transactionId":5}`, `{}`, ""},

		// Open transaction for replay
		{"openTransaction", `{"name": "foo", "args": [], "rebaseOpts": {"basis": "e99uif9c7bpavajrt666es1ki52dv239", "original": "e99uif9c7bpavajrt666es1ki52dv239"}}`, ``, "only local mutations"}, // bad basis
		{"openTransaction", `{"name": "foo", "args": [], "rebaseOpts": {"basis": "", "original": "e99uif9c7bpavajrt666es1ki52dv239"}}`, ``, "Invaild hash"},                                         // no basis
		{"openTransaction", `{"name": "foo", "args": [], "rebaseOpts": {"basis": "e99uif9c7bpavajrt666es1ki52dv239", "original": "0000000000pavajrt666es1ki52dv239"}}`, ``, "not found"},            // bad original
		{"openTransaction", `{"name": "foo", "args": [], "rebaseOpts": {"basis": "e99uif9c7bpavajrt666es1ki52dv239", "original": "ir8von08b95s17t04gjk6qjas4m7om78"}}`, `{"transactionId":8}`, ""},  // good case
		{"put", `{"transactionId": 8, "key": "foom", "value": "fomo"}`, `{}`, ""},
		{"commitTransaction", `{"transactionId":8}`, `{"ref":"0904mtoobeg7g0m833e03kn6abaa7022"}`, ""},

		// TODO: other scan operators
	}

	for _, t := range tc {
		res, err := Dispatch("db1", t.rpc, []byte(t.req))
		if t.expectedError != "" {
			assert.Nil(res, "test case %s: %s", t.rpc, t.req, "test case %s: %s", t.rpc, t.req)
			assert.Regexp(t.expectedError, err.Error(), "test case %s: %s", t.rpc, t.req)
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
		buf, err := Dispatch("db1", "pullProgress", mustMarshal(pullProgressRequest{}))
		assert.NoError(err)
		var resp pullProgressResponse
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
	req := pullRequest{
		Remote: jsnoms.Spec{sp},
	}

	_, err = Dispatch("db1", "pull", mustMarshal(req))
	assert.Regexp(`is not valid JSON`, err.Error())
}
