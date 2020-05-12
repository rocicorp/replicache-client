package repm

import (
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"

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
		{"commitTransaction", `{"transactionId":1}`, `{"ref":"hafgie633fm1pg70olfum414ossa6mt6"}`, ""},
		{"getRoot", `{}`, `{"root":"hafgie633fm1pg70olfum414ossa6mt6"}`, ""}, // getRoot when db did change

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
		{"commitTransaction", `{"transactionId":4}`, `{"ref":"3enaqu4u7lfn58th9b3dnfp90sf9nrc2"}`, ""},
		{"openTransaction", `{}`, `{"transactionId":5}`, ""},
		{"scan", `{"transactionId": 5, "prefix": "foo"}`, `[{"key":"foo","value":"bar"},{"key":"foopa","value":"doopa"}]`, ""},
		{"scan", `{"transactionId": 5, "start": {"id": {"value": "foo"}}}`, `[{"key":"foo","value":"bar"},{"key":"foopa","value":"doopa"}]`, ""},
		{"scan", `{"transactionId": 5, "start": {"id": {"value": "foo", "exclusive": true}}}`, `[{"key":"foopa","value":"doopa"}]`, ""},
		{"closeTransaction", `{"transactionId":5}`, `{}`, ""},

		// Open transaction for replay
		{"openTransaction", `{"name": "foo", "args": [], "rebaseOpts": {"basis": "e99uif9c7bpavajrt666es1ki52dv239", "original": "e99uif9c7bpavajrt666es1ki52dv239"}}`, ``, "only local mutations"}, // bad basis
		{"openTransaction", `{"name": "foo", "args": [], "rebaseOpts": {"basis": "", "original": "e99uif9c7bpavajrt666es1ki52dv239"}}`, ``, "Invaild hash"},                                         // no basis
		{"openTransaction", `{"name": "foo", "args": [], "rebaseOpts": {"basis": "e99uif9c7bpavajrt666es1ki52dv239", "original": "0000000000pavajrt666es1ki52dv239"}}`, ``, "not found"},            // bad original
		{"openTransaction", `{"name": "foo", "args": [], "rebaseOpts": {"basis": "e99uif9c7bpavajrt666es1ki52dv239", "original": "3enaqu4u7lfn58th9b3dnfp90sf9nrc2"}}`, `{"transactionId":8}`, ""},  // good case
		{"put", `{"transactionId": 8, "key": "foom", "value": "fomo"}`, `{}`, ""},
		{"commitTransaction", `{"transactionId":8}`, `{"ref":"cafum2tsootnip1me4ltfqme0q25ekk8"}`, ""},

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
