package db

import (
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
		name             string
		input            []Local
		reqError         bool
		respCode         int
		respBody         string
		expStatusCode    int
		expErrorMessage  string
		expMutationInfos []MutationInfo
	}{
		{
			"nothing to do",
			[]Local{},
			false,
			200,
			`{}`,
			200,
			"",
			nil,
		},
		{
			"success",
			[]Local{
				{MutationID: 1, Name: "name1", Args: types.NewList(db.noms, types.Number(1))},
				{MutationID: 2, Name: "name2", Args: types.NewList(db.noms, types.Number(2))},
			},
			false,
			200,
			`{"mutationInfos": [{"ID": 1}]}`,
			200,
			"",
			[]MutationInfo{
				{ID: 1},
			},
		},
		{
			"request error",
			[]Local{},
			true,
			0,
			``,
			0,
			"connect: connection refused",
			nil,
		},
		{
			"403",
			[]Local{},
			false,
			403,
			`Unauthorized`,
			403,
			"Unauthorized",
			nil,
		},
		{
			"empty response",
			[]Local{},
			false,
			200,
			``,
			200,
			"error decoding batch push response: EOF",
			nil,
		},
		{
			"malformed response",
			[]Local{},
			false,
			200,
			`not json`,
			200,
			"error decoding batch push response: invalid character 'o' in literal null",
			nil,
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
				v, err := nomsjson.FromJSON(req.Mutations[i].Args, db.noms)
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
			got := defaultPusher{}.Push(tt.input, server.URL, dataLayerAuth, obfuscatedClientID)
			assert.Equal(tt.expStatusCode, got.HTTPStatusCode)
			assert.Equal(tt.expMutationInfos, got.BatchPushResponse.MutationInfos)
			assert.Regexp(tt.expErrorMessage, got.ErrorMessage)
		})
	}
}
