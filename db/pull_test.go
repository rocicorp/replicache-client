package db

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/datetime"
	"github.com/stretchr/testify/assert"
	"roci.dev/diff-server/kv"
	servetypes "roci.dev/diff-server/serve/types"
	nomsjson "roci.dev/diff-server/util/noms/json"
)

func TestBaseSnapshot(t *testing.T) {
	assert := assert.New(t)
	d := datetime.Now()
	db, _ := LoadTempDB(assert)

	var commits testCommits
	commits.addGenesis(assert, db)
	c, err := baseSnapshot(db.noms, commits.head())
	assert.NoError(err)
	assert.True(commits.genesis().NomsStruct.Equals(c.NomsStruct))

	commits.addLocal(assert, db, d).addLocal(assert, db, d)
	c, err = baseSnapshot(db.noms, commits.head())
	assert.NoError(err)
	assert.True(commits.genesis().NomsStruct.Equals(c.NomsStruct))

	commits.addSnapshot(assert, db)
	expSnapshot := commits.head()
	commits.addLocal(assert, db, d)
	c, err = baseSnapshot(db.noms, commits.head())
	assert.NoError(err)
	assert.True(expSnapshot.NomsStruct.Equals(c.NomsStruct))
}

func TestPull(t *testing.T) {
	assert := assert.New(t)

	tc := []struct {
		label                            string
		initialState                     map[string]string
		initialStateID                   string
		reqError                         bool
		respCode                         int
		respBody                         string
		expectedError                    string
		expectedData                     map[string]string
		expectedBaseServerStateID        string
		expectedLastMutationID           uint64
		expectedClientViewHTTPStatusCode int
		expectedClientViewErrorMessage   string
	}{
		{
			"ok-nop",
			map[string]string{},
			"",
			false,
			http.StatusOK,
			`{"patch":[],"stateID":"11111111111111111111111111111111","checksum":"00000000","lastMutationID":2,"clientViewInfo":{"httpStatusCode":200,"errorMessage":""}}`,
			"",
			map[string]string{},
			"11111111111111111111111111111111",
			2,
			200,
			"",
		},
		{
			"ok-no-basis",
			map[string]string{},
			"",
			false,
			http.StatusOK,
			`{"patch":[{"op":"add","path":"/foo","value":"bar"}],"stateID":"11111111111111111111111111111111","checksum":"c4e7090d","lastMutationID":2,"clientViewInfo":{"httpStatusCode":200,"errorMessage":""}}`,
			"",
			map[string]string{"foo": `"bar"`},
			"11111111111111111111111111111111",
			2,
			200,
			"",
		},
		{
			"ok-with-basis",
			map[string]string{},
			"11111111111111111111111111111111",
			false,
			http.StatusOK,
			`{"patch":[{"op":"add","path":"/foo","value":"bar"}],"stateID":"22222222222222222222222222222222","checksum":"c4e7090d","lastMutationID":3,"clientViewInfo":{"httpStatusCode":200,"errorMessage":""}}`,
			"",
			map[string]string{"foo": `"bar"`},
			"22222222222222222222222222222222",
			3,
			200,
			"",
		},
		{
			"network-error",
			map[string]string{"foo": `"bar"`},
			"11111111111111111111111111111111",
			true,
			0,
			``,
			`Post "?http://127.0.0.1:\d+/pull"?: dial tcp 127.0.0.1:\d+: connect: connection refused`,
			map[string]string{},
			"",
			0,
			0,
			"",
		},
		{
			"http-error",
			map[string]string{"foo": `"bar"`},
			"11111111111111111111111111111111",
			false,
			http.StatusBadRequest,
			"You have made an invalid request",
			"400 Bad Request: You have made an invalid request",
			map[string]string{},
			"",
			0,
			0,
			"",
		},
		{
			"invalid-response",
			map[string]string{"foo": `"bar"`},
			"11111111111111111111111111111111",
			false,
			http.StatusOK,
			"this isn't valid json!",
			`response from http://127.0.0.1:\d+/pull is not valid JSON: invalid character 'h' in literal true \(expecting 'r'\)`,
			map[string]string{},
			"",
			0,
			0,
			"",
		},
		{
			"empty-response",
			map[string]string{"foo": `"bar"`},
			"11111111111111111111111111111111",
			false,
			http.StatusOK,
			"",
			`response from http://127.0.0.1:\d+/pull is not valid JSON: EOF`,
			map[string]string{},
			"",
			0,
			0,
			"",
		},
		{
			"nuke-first-patch",
			map[string]string{"foo": `"bar"`},
			"11111111111111111111111111111111",
			false,
			http.StatusOK,
			`{"patch":[{"op":"remove","path":"/"},{"op":"add","path":"/foo","value":"baz"}],"stateID":"22222222222222222222222222222222","checksum":"0c3e8305","lastMutationID":2}`,
			"",
			map[string]string{"foo": `"baz"`},
			"22222222222222222222222222222222",
			2,
			0,
			"",
		},
		{
			"invalid-patch-nuke-late-patch",
			map[string]string{"foo": `"bar"`},
			"11111111111111111111111111111111",
			false,
			http.StatusOK,
			`{"patch":[{"op":"add","path":"/foo","value":"baz"},{"op":"remove","path":""}],"stateID":"22222222222222222222222222222222","checksum":"c4e7090d","lastMutationID":2}`,
			"couldn't apply patch",
			map[string]string{},
			"",
			0,
			0,
			"",
		},
		{
			"invalid-patch-bad-op",
			map[string]string{"foo": `"bar"`},
			"11111111111111111111111111111111",
			false,
			http.StatusOK,
			`{"patch":[{"op":"add","path":"/foo"}],"stateID":"22222222222222222222222222222222","checksum":"c4e7090d","lastMutationID":2}`,
			"couldn't apply patch: couldnt parse value from JSON '': couldn't parse value '' as json: unexpected end of JSON input",
			map[string]string{},
			"",
			0,
			0,
			"",
		},
		{
			"invalid-patch-bad-op",
			map[string]string{"foo": `"bar"`},
			"11111111111111111111111111111111",
			false,
			http.StatusOK,
			`{"patch":[{"op":"monkey"}],"stateID":"22222222222222222222222222222222","checksum":"c4e7090d","lastMutationID":2}`,
			"couldn't apply patch: Invalid path",
			map[string]string{},
			"",
			0,
			0,
			"",
		},
		{
			"checksum-mismatch",
			map[string]string{},
			"11111111111111111111111111111111",
			false,
			http.StatusOK,
			`{"patch":[{"op":"add","path":"/u/foo","value":"bar"}],"stateID":"22222222222222222222222222222222","checksum":"aaaaaaaa","lastMutationID":2}`,
			"checksum mismatch!",
			map[string]string{},
			"",
			0,
			0,
			"",
		},
		{
			"auth-error",
			map[string]string{},
			"",
			false,
			http.StatusNotImplemented,
			`Response Body`,
			"Not Implemented: Response Body",
			map[string]string{},
			"",
			0,
			0,
			"",
		},
		{
			"client-view-info",
			map[string]string{},
			"",
			false,
			http.StatusOK,
			`{"patch":[],"stateID":"11111111111111111111111111111111","checksum":"00000000","lastMutationID":2,"clientViewInfo":{"httpStatusCode":234,"errorMessage":"Xyz"}}`,
			"",
			map[string]string{},
			"11111111111111111111111111111111",
			2,
			234,
			"Xyz",
		},
		{
			"ensure-put-canonicalizes",
			map[string]string{"foo": `"bar"`},
			"11111111111111111111111111111111",
			false,
			http.StatusOK,
			`{"patch":[{"op":"remove","path":"/"},{"op":"add","path":"/foo","value":"\u000b"}],"stateID":"22222222222222222222222222222222","checksum":"6206e20c","lastMutationID":2}`,
			"",
			map[string]string{"foo": `"\u000B"`}, // \u000B is canonical for \u000b which was returned
			"22222222222222222222222222222222",
			2,
			0,
			"",
		},
		{
			"refuse-to-travel-backwards-in-time",
			map[string]string{"foo": `"bar"`},
			"11111111111111111111111111111111",
			false,
			http.StatusOK,
			`{"patch":[{"op":"remove","path":"/"},{"op":"add","path":"/foo","value":"oldvalue"}],"stateID":"22222222222222222222222222222222","checksum":"a745e22b","lastMutationID":0}`,
			"client view lastMutationID 0 is < previous lastMutationID 1; ignoring",
			map[string]string{},
			"11111111111111111111111111111111",
			1,
			0,
			"",
		},
	}

	for _, t := range tc {
		db, dir := LoadTempDB(assert)
		fmt.Println("dir", dir)

		ed := kv.NewMap(db.noms).Edit()
		if t.initialState != nil {
			for k, v := range t.initialState {
				v, err := nomsjson.FromJSON([]byte(v), db.Noms())
				assert.NoError(err)
				assert.NoError(ed.Set(types.String(k), v))
			}
		}
		m := ed.Build()
		g := makeGenesis(db.noms, t.initialStateID, db.noms.WriteValue(m.NomsMap()), m.NomsChecksum(), 1 /*lastMutationID*/)
		_, err := db.noms.SetHead(db.noms.GetDataset(MASTER_DATASET), db.noms.WriteValue(g.NomsStruct))
		assert.NoError(err)
		err = db.Reload()
		assert.NoError(err, t.label)

		clientViewAuth := "t123"

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var reqBody servetypes.PullRequest
			err := json.NewDecoder(r.Body).Decode(&reqBody)
			assert.NoError(err, t.label)
			assert.Equal(t.initialStateID, reqBody.BaseStateID, t.label)
			assert.Equal("diffServerAuth", r.Header.Get("Authorization"))
			assert.NotEqual("", reqBody.ClientID)
			assert.Equal(clientViewAuth, reqBody.ClientViewAuth)
			w.WriteHeader(t.respCode)
			w.Write([]byte(t.respBody))
		}))

		if t.reqError {
			server.Close()
		}

		puller := &defaultPuller{}
		gotSnapshot, cvi, err := puller.Pull(db.noms, g, fmt.Sprintf("%s/pull", server.URL), "diffServerAuth", clientViewAuth, db.clientID)
		if t.expectedError == "" {
			assert.NoError(err, t.label)
			assert.NotEqual(Commit{}, gotSnapshot)
		} else {
			assert.Error(err, t.label)
			assert.Regexp(t.expectedError, err.Error(), t.label)
		}
		assert.Equal(t.expectedClientViewHTTPStatusCode, cvi.HTTPStatusCode)
		assert.Equal(t.expectedClientViewErrorMessage, cvi.ErrorMessage)

		ee := kv.NewMap(db.noms).Edit()
		for k, v := range t.expectedData {
			v, err := nomsjson.FromJSON([]byte(v), db.Noms())
			assert.NoError(err)
			assert.NoError(ee.Set(types.String(k), v), t.label)
		}
		expected := ee.Build()
		if t.expectedError == "" {
			gotChecksum := gotSnapshot.Data(db.noms).Checksum()
			assert.Equal(expected.Checksum(), gotChecksum, t.label)
			assert.True(expected.NomsMap().Equals(gotSnapshot.Data(db.noms)))
			assert.Equal(t.expectedBaseServerStateID, gotSnapshot.Meta.Snapshot.ServerStateID, t.label)
			assert.Equal(t.expectedLastMutationID, gotSnapshot.Meta.Snapshot.LastMutationID, t.label)
		} else {
			assert.Equal(Commit{}, gotSnapshot)
		}
	}
}
