package db

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/attic-labs/noms/go/types"
	"github.com/stretchr/testify/assert"
	nomsjson "roci.dev/diff-server/util/noms/json"
)

func Test_push(t *testing.T) {
	assert := assert.New(t)
	db, _ := LoadTempDB(assert)

	type args struct {
		pending            []Local
		url                string
		clientViewAuth     string
		obfuscatedClientID string
	}
	tests := []struct {
		name        string
		input       []Local
		reqError    bool
		respCode    int
		respBody    string
		expected    BatchPushInfo
		expectedErr string
	}{
		{
			"nothing to do",
			[]Local{},
			false,
			200,
			`{}`,
			BatchPushInfo{HTTPStatusCode: 200},
			"",
		},
		{
			"success",
			[]Local{
				{MutationID: 1, Name: "name1", Args: types.NewList(db.noms, types.Number(1))},
				{MutationID: 2, Name: "name2", Args: types.NewList(db.noms, types.Number(2))},
			},
			false,
			200,
			`{"mutationInfos": [{"ID": "1"}]}`,
			BatchPushInfo{
				HTTPStatusCode: 200,
				BatchPushResponse: BatchPushResponse{
					MutationInfos: []MutationInfo{
						{ID: "1"},
					}}},
			"",
		},
		{
			"request error",
			[]Local{},
			true,
			0,
			``,
			BatchPushInfo{},
			"connect",
		},
		{
			"403",
			[]Local{},
			false,
			403,
			`Unauthorized`,
			BatchPushInfo{HTTPStatusCode: 403, ErrorMessage: "Unauthorized"},
			"",
		},
		{
			"empty response",
			[]Local{},
			false,
			200,
			``,
			BatchPushInfo{HTTPStatusCode: 200, ErrorMessage: "error decoding batch push response: EOF"},
			"",
		},
		{
			"malformed response",
			[]Local{},
			false,
			200,
			`not json`,
			BatchPushInfo{HTTPStatusCode: 200, ErrorMessage: "error decoding batch push response: invalid character 'o' in literal null (expecting 'u')"},
			"",
		},
	}
	for _, tt := range tests {
		dataLayerAuth := "data layer auth token"
		obfuscatedClientID := "obfuscated client id"

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var req BatchPushRequest
			err := json.NewDecoder(r.Body).Decode(&req)
			assert.NoError(err, tt.name)
			assert.Equal(obfuscatedClientID, req.ClientID)
			assert.Equal(len(tt.input), len(req.Mutations))
			for i := range req.Mutations {
				assert.Equal(tt.input[i].MutationID, req.Mutations[i].ID)
				assert.Equal(tt.input[i].Name, req.Mutations[i].Name)
				v, err := nomsjson.FromJSON(bytes.NewReader(req.Mutations[i].Args), db.noms)
				assert.NoError(err)
				assert.True(v.Equals(tt.input[i].Args))
			}
			assert.Equal(dataLayerAuth, r.Header.Get("Authorization"))
			w.WriteHeader(tt.respCode)
			w.Write([]byte(tt.respBody))
		}))

		if tt.reqError {
			server.Close()
		}

		t.Run(tt.name, func(t *testing.T) {
			got, err := defaultPusher{}.Push(tt.input, server.URL, dataLayerAuth, obfuscatedClientID)
			if tt.expectedErr != "" {
				assert.Error(err, tt.name)
				if err != nil {
					assert.Regexp(tt.expectedErr, err.Error())
				}
			} else {
				assert.Equal(tt.expected, got)
			}
		})
	}
}
