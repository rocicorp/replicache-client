package db

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/attic-labs/noms/go/marshal"
	"github.com/attic-labs/noms/go/spec"
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
	assert.True(commits.genesis().Original.Equals(c.Original))

	commits.addLocal(assert, db, d).addLocal(assert, db, d)
	c, err = baseSnapshot(db.noms, commits.head())
	assert.NoError(err)
	assert.True(commits.genesis().Original.Equals(c.Original))

	commits.addSnapshot(assert, db)
	expSnapshot := commits.head()
	commits.addLocal(assert, db, d)
	c, err = baseSnapshot(db.noms, commits.head())
	assert.NoError(err)
	assert.True(expSnapshot.Original.Equals(c.Original))
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
			"couldn't apply patch: couldnt parse value from JSON '': EOF",
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
				v, err := nomsjson.FromJSON(strings.NewReader(v), db.Noms())
				assert.NoError(err)
				assert.NoError(ed.Set(types.String(k), v))
			}
		}
		m := ed.Build()
		g := makeGenesis(db.noms, t.initialStateID, db.noms.WriteValue(m.NomsMap()), m.NomsChecksum(), 1 /*lastMutationID*/)
		_, err := db.noms.SetHead(db.noms.GetDataset(LOCAL_DATASET), db.noms.WriteValue(g.Original))
		assert.NoError(err)
		err = db.Reload()
		assert.NoError(err, t.label)

		clientViewAuth := "t123"

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var reqBody servetypes.PullRequest
			err := json.NewDecoder(r.Body).Decode(&reqBody)
			assert.NoError(err, t.label)
			assert.Equal(t.initialStateID, reqBody.BaseStateID, t.label)
			assert.Equal("sandbox", r.Header.Get("Authorization"))
			assert.NotEqual("", reqBody.ClientID)
			assert.Equal(clientViewAuth, reqBody.ClientViewAuth)
			w.WriteHeader(t.respCode)
			w.Write([]byte(t.respBody))
		}))

		if t.reqError {
			server.Close()
		}

		gotSnapshot, cvi, err := pull(db.noms, g, fmt.Sprintf("%s/pull", server.URL), clientViewAuth, db.clientID)
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
			v, err := nomsjson.FromJSON(strings.NewReader(v), db.Noms())
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

func TestDoomedDBPull(t *testing.T) {
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
			http.StatusOK,
			``,
			`Post "?http://127.0.0.1:\d+/pull"?: dial tcp 127.0.0.1:\d+: connect: connection refused`,
			map[string]string{"foo": `"bar"`},
			"11111111111111111111111111111111",
			1,
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
			map[string]string{"foo": `"bar"`},
			"11111111111111111111111111111111",
			1,
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
			`Response from http://127.0.0.1:\d+/pull is not valid JSON: invalid character 'h' in literal true \(expecting 'r'\)`,
			map[string]string{"foo": `"bar"`},
			"11111111111111111111111111111111",
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
			`Response from http://127.0.0.1:\d+/pull is not valid JSON: EOF`,
			map[string]string{"foo": `"bar"`},
			"11111111111111111111111111111111",
			1,
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
			"couldnt apply patch",
			map[string]string{"foo": `"bar"`},
			"11111111111111111111111111111111",
			1,
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
			"couldnt apply patch: couldnt parse value from JSON '': EOF",
			map[string]string{"foo": `"bar"`},
			"11111111111111111111111111111111",
			1,
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
			"couldnt apply patch: Invalid path",
			map[string]string{"foo": `"bar"`},
			"11111111111111111111111111111111",
			1,
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
			"Checksum mismatch!",
			map[string]string{},
			"11111111111111111111111111111111",
			1,
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
			1,
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
			"Client view lastMutationID 0 is < previous lastMutationID 1; ignoring",
			map[string]string{"foo": `"bar"`},
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
				v, err := nomsjson.FromJSON(strings.NewReader(v), db.Noms())
				assert.NoError(err)
				assert.NoError(ed.Set(types.String(k), v))
			}
		}
		m := ed.Build()
		g := makeGenesis(db.noms, t.initialStateID, db.noms.WriteValue(m.NomsMap()), m.NomsChecksum(), 1 /*lastMutationID*/)
		_, err := db.noms.SetHead(db.noms.GetDataset(LOCAL_DATASET), db.noms.WriteValue(marshal.MustMarshal(db.noms, g)))
		assert.NoError(err)
		err = db.Reload()
		assert.NoError(err, t.label)

		clientViewAuth := "t123"

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var reqBody servetypes.PullRequest
			err := json.NewDecoder(r.Body).Decode(&reqBody)
			assert.NoError(err, t.label)
			assert.Equal(t.initialStateID, reqBody.BaseStateID, t.label)
			assert.Equal("sandbox", r.Header.Get("Authorization"))
			assert.NotEqual("", reqBody.ClientID)
			assert.Equal(clientViewAuth, reqBody.ClientViewAuth)
			w.WriteHeader(t.respCode)
			w.Write([]byte(t.respBody))
		}))

		if t.reqError {
			server.Close()
		}

		sp, err := spec.ForDatabase(server.URL)
		assert.NoError(err, t.label)

		cvi, err := db.Pull(sp, clientViewAuth, nil)
		if t.expectedError == "" {
			assert.NoError(err, t.label)
		} else {
			assert.Regexp(t.expectedError, err.Error(), t.label)
		}
		assert.Equal(t.expectedClientViewHTTPStatusCode, cvi.HTTPStatusCode)
		assert.Equal(t.expectedClientViewErrorMessage, cvi.ErrorMessage)

		ee := kv.NewMap(db.noms).Edit()
		for k, v := range t.expectedData {
			v, err := nomsjson.FromJSON(strings.NewReader(v), db.Noms())
			assert.NoError(err)
			assert.NoError(ee.Set(types.String(k), v), t.label)
		}
		expected := ee.Build()
		gotChecksum, err := kv.ChecksumFromString(string(db.head.Value.Checksum))
		assert.NoError(err)
		assert.Equal(expected.Checksum(), gotChecksum.String(), t.label)

		if t.expectedError == "" {
			assert.Equal(t.expectedBaseServerStateID, db.head.Meta.Snapshot.ServerStateID, t.label)
			assert.Equal(t.expectedLastMutationID, db.head.Meta.Snapshot.LastMutationID, t.label)
		}
	}
}

func TestProgressWhichIsDoomed(t *testing.T) {
	oneChunk := [][]byte{[]byte(`"foo"`)}
	twoChunks := [][]byte{[]byte(`"foo`), []byte(`bar"`)}

	total := func(chunks [][]byte) uint64 {
		t := uint64(0)
		for _, c := range chunks {
			t += uint64(len(c))
		}
		return t
	}

	tc := []struct {
		hasProgressHandler bool
		sendContentLength  bool
		sendEntityLength   bool
		chunks             [][]byte
	}{
		{false, false, false, oneChunk},
		{true, false, false, oneChunk},
		{false, true, false, oneChunk},
		{false, false, true, oneChunk},
		{true, true, false, oneChunk},
		{true, false, true, oneChunk},
		{false, true, true, oneChunk},
		{true, true, true, oneChunk},
		{false, false, false, twoChunks},
		{true, false, false, twoChunks},
		{false, true, false, twoChunks},
		{false, false, true, twoChunks},
		{true, true, false, twoChunks},
		{true, false, true, twoChunks},
		{false, true, true, twoChunks},
		{true, true, true, twoChunks},
	}

	assert := assert.New(t)
	db, dir := LoadTempDB(assert)
	fmt.Println("dir", dir)

	for i, t := range tc {
		label := fmt.Sprintf("test case %d", i)

		type report struct {
			received uint64
			expected uint64
		}
		reports := []report{}
		var progress Progress
		if t.hasProgressHandler {
			progress = func(bytesReceived, bytesExpected uint64) {
				reports = append(reports, report{bytesReceived, bytesExpected})
			}
		}

		totalLen := total(t.chunks)
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if t.sendEntityLength {
				w.Header().Set("Entity-length", fmt.Sprintf("%d", totalLen))
			}
			if t.sendContentLength {
				w.Header().Set("Content-length", fmt.Sprintf("%d", totalLen))
			}

			for _, c := range t.chunks {
				_, err := w.Write(c)
				assert.NoError(err, label)
				w.(http.Flusher).Flush()
				// This is a little ghetto. Doing fancier things with channel locking was too hard.
				time.Sleep(time.Millisecond)
			}
		}))

		clientViewAuth := "test-2"
		sp, err := spec.ForDatabase(server.URL)
		assert.NoError(err, label)
		_, err = db.Pull(sp, clientViewAuth, progress)
		assert.Regexp(`Response from http://[\d\.\:]+/pull is not valid JSON`, err)

		expected := []report{}
		if t.hasProgressHandler {
			soFar := uint64(0)
			for _, c := range t.chunks {
				soFar += uint64(len(c))
				expectedLen := soFar
				if t.sendEntityLength || t.sendContentLength {
					expectedLen = totalLen
				}
				expected = append(expected, report{
					received: soFar,
					expected: expectedLen,
				})
			}
			// If there's no content length, the reader gets called one extra time to figure out it's at the end.
			if !t.sendContentLength {
				expected = append(expected, expected[len(expected)-1])
			}
		}
		assert.Equal(expected, reports, label)
	}
}
