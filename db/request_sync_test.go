package db

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/attic-labs/noms/go/marshal"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/stretchr/testify/assert"
	"roci.dev/diff-server/kv"
	servetypes "roci.dev/diff-server/serve/types"
)

func TestRequestSync(t *testing.T) {
	assert := assert.New(t)

	tc := []struct {
		label                     string
		initialState              map[string]string
		initialStateID            string
		reqError                  bool
		respCode                  int
		respBody                  string
		expectedError             string
		expectedErrorIsAuthError  bool
		expectedData              map[string]string
		expectedBaseServerStateID string
		expectedLastTransactionID string
	}{
		{
			"ok-nop",
			map[string]string{},
			"",
			false,
			http.StatusOK,
			`{"patch":[],"stateID":"11111111111111111111111111111111","checksum":"00000000","lastTransactionID":"1"}`,
			"",
			false,
			map[string]string{},
			"11111111111111111111111111111111",
			"1",
		},
		{
			"ok-no-basis",
			map[string]string{},
			"",
			false,
			http.StatusOK,
			`{"patch":[{"op":"add","path":"/foo","value":"bar"}],"stateID":"11111111111111111111111111111111","checksum":"c4e7090d","lastTransactionID":"1"}`,
			"",
			false,
			map[string]string{"foo": `"bar"`},
			"11111111111111111111111111111111",
			"1",
		},
		{
			"ok-with-basis",
			map[string]string{},
			"11111111111111111111111111111111",
			false,
			http.StatusOK,
			`{"patch":[{"op":"add","path":"/foo","value":"bar"}],"stateID":"22222222222222222222222222222222","checksum":"c4e7090d","lastTransactionID":"2"}`,
			"",
			false,
			map[string]string{"foo": `"bar"`},
			"22222222222222222222222222222222",
			"2",
		},
		{
			"network-error",
			map[string]string{"foo": `"bar"`},
			"11111111111111111111111111111111",
			true,
			http.StatusOK,
			``,
			`Post "?http://127.0.0.1:\d+/handlePull"?: dial tcp 127.0.0.1:\d+: connect: connection refused`,
			false,
			map[string]string{"foo": `"bar"`},
			"11111111111111111111111111111111",
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
			false,
			map[string]string{"foo": `"bar"`},
			"11111111111111111111111111111111",
			"",
		},
		{
			"invalid-response",
			map[string]string{"foo": `"bar"`},
			"11111111111111111111111111111111",
			false,
			http.StatusOK,
			"this isn't valid json!",
			`Response from http://127.0.0.1:\d+/handlePull is not valid JSON: invalid character 'h' in literal true \(expecting 'r'\)`,
			false,
			map[string]string{"foo": `"bar"`},
			"11111111111111111111111111111111",
			"",
		},
		{
			"empty-response",
			map[string]string{"foo": `"bar"`},
			"11111111111111111111111111111111",
			false,
			http.StatusOK,
			"",
			`Response from http://127.0.0.1:\d+/handlePull is not valid JSON: EOF`,
			false,
			map[string]string{"foo": `"bar"`},
			"11111111111111111111111111111111",
			"",
		},
		{
			"nuke-first-patch",
			map[string]string{"foo": `"bar"`},
			"11111111111111111111111111111111",
			false,
			http.StatusOK,
			`{"patch":[{"op":"remove","path":"/"},{"op":"add","path":"/foo","value":"baz"}],"stateID":"22222222222222222222222222222222","checksum":"0c3e8305","lastTransactionID":"2"}`,
			"",
			false,
			map[string]string{"foo": `"baz"`},
			"22222222222222222222222222222222",
			"2",
		},
		{
			"invalid-patch-nuke-late-patch",
			map[string]string{"foo": `"bar"`},
			"11111111111111111111111111111111",
			false,
			http.StatusOK,
			`{"patch":[{"op":"add","path":"/foo","value":"baz"},{"op":"remove","path":""}],"stateID":"22222222222222222222222222222222","checksum":"c4e7090d"}`,
			"couldnt apply patch",
			false,
			map[string]string{"foo": `"bar"`},
			"11111111111111111111111111111111",
			"",
		},
		{
			"invalid-patch-bad-op",
			map[string]string{"foo": `"bar"`},
			"11111111111111111111111111111111",
			false,
			http.StatusOK,
			`{"patch":[{"op":"add","path":"/foo"}],"stateID":"22222222222222222222222222222222","checksum":"c4e7090d","lastTransactionID":"1"}`,
			"couldnt apply patch: EOF",
			false,
			map[string]string{"foo": `"bar"`},
			"11111111111111111111111111111111",
			"",
		},
		{
			"invalid-patch-bad-op",
			map[string]string{"foo": `"bar"`},
			"11111111111111111111111111111111",
			false,
			http.StatusOK,
			`{"patch":[{"op":"monkey"}],"stateID":"22222222222222222222222222222222","checksum":"c4e7090d","lastTransactionID":"1"}`,
			"couldnt apply patch: Invalid path",
			false,
			map[string]string{"foo": `"bar"`},
			"11111111111111111111111111111111",
			"",
		},
		{
			"checksum-mismatch",
			map[string]string{},
			"11111111111111111111111111111111",
			false,
			http.StatusOK,
			`{"patch":[{"op":"add","path":"/u/foo","value":"bar"}],"stateID":"22222222222222222222222222222222","checksum":"aaaaaaaa"},"lastTransactionID":"1"`,
			"Checksum mismatch!",
			false,
			map[string]string{},
			"11111111111111111111111111111111",
			"",
		},
		{
			"auth-error",
			map[string]string{},
			"",
			false,
			http.StatusForbidden,
			`Bad auth token`,
			"Forbidden: Bad auth token",
			true,
			map[string]string{},
			"",
			"",
		},
	}

	for _, t := range tc {
		db, dir := LoadTempDB(assert)
		fmt.Println("dir", dir)

		ed := kv.NewMap(db.noms).Edit()
		if t.initialState != nil {
			for k, v := range t.initialState {
				assert.NoError(ed.Set(k, []byte(v)))
			}
		}
		m := ed.Build()
		g := makeGenesis(db.noms, t.initialStateID, db.noms.WriteValue(m.NomsMap()), types.String(m.Checksum().String()), "" /*lastTransactionID*/)
		_, err := db.noms.SetHead(db.noms.GetDataset(LOCAL_DATASET), db.noms.WriteValue(marshal.MustMarshal(db.noms, g)))
		assert.NoError(err)
		err = db.Reload()
		assert.NoError(err, t.label)

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var reqBody servetypes.PullRequest
			err := json.NewDecoder(r.Body).Decode(&reqBody)
			assert.NoError(err, t.label)
			assert.Equal(t.initialStateID, reqBody.BaseStateID, t.label)
			w.WriteHeader(t.respCode)
			w.Write([]byte(t.respBody))
		}))

		if t.reqError {
			server.Close()
		}

		sp, err := spec.ForDatabase(server.URL)
		assert.NoError(err, t.label)

		err = db.RequestSync(sp, nil)
		if t.expectedError == "" {
			assert.NoError(err, t.label)
		} else {
			assert.Regexp(t.expectedError, err.Error(), t.label)
			_, ok := err.(PullAuthError)
			assert.Equal(t.expectedErrorIsAuthError, ok, t.label)
		}

		ee := kv.NewMap(db.noms).Edit()
		for k, v := range t.expectedData {
			assert.NoError(ee.Set(k, []byte(v)), t.label)
		}
		expected := ee.Build()
		gotChecksum, err := kv.ChecksumFromString(string(db.head.Value.Checksum))
		assert.NoError(err)
		assert.True(expected.Checksum().Equal(*gotChecksum), t.label)

		assert.Equal(t.expectedBaseServerStateID, db.head.Meta.Genesis.ServerStateID, t.label)
		assert.Equal(t.expectedLastTransactionID, db.head.Meta.Genesis.LastTransactionID, t.label)
	}
}

func TestProgress(t *testing.T) {
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

		sp, err := spec.ForDatabase(server.URL)
		assert.NoError(err, label)
		err = db.RequestSync(sp, progress)
		assert.Regexp(`Response from http://[\d\.\:]+/handlePull is not valid JSON`, err)

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
