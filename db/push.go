package db

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	nomsjson "roci.dev/diff-server/util/noms/json"
)


type BatchPushRequest struct {
	ClientID  string     `json:"clientId"`
	Mutations []Mutation `json:"mutations"`
}

// Public because returned in the MaybeEndSyncResponse.
type Mutation struct {
	ID   uint64          `json:"id"`
	Name string          `json:"name"`
	Args json.RawMessage `json:"args"`
}

type BatchPushResponse struct {
	MutationInfos []MutationInfo `json:"mutationInfos"`
}

type MutationInfo struct {
	ID    string `json:"id"`
	Error string `json:"error"`
}

type BatchPushInfo struct {
	HTTPStatusCode    int               `json:"httpStatusCode"`
	ErrorMessage      string            `json:"errorMessage"`
	BatchPushResponse BatchPushResponse `json:"batchPushResponse"`
}

type pusher interface {
	Push(pending []Local, url string, dataLayerAuth string, obfuscatedClientID string) (BatchPushInfo, error)
}

type defaultPusher struct{}

func (defaultPusher) Push(pending []Local, url string, dataLayerAuth string, obfuscatedClientID string) (BatchPushInfo, error) {
	var req BatchPushRequest
	req.ClientID = obfuscatedClientID
	for _, p := range pending {
		var args bytes.Buffer
		if err := nomsjson.ToJSON(p.Args, &args); err != nil {
			return BatchPushInfo{}, err
		}
		req.Mutations = append(req.Mutations, Mutation{p.MutationID, p.Name, args.Bytes()})
	}
	reqBody, err := json.Marshal(req)
	if err != nil {
		return BatchPushInfo{}, err
	}

	httpReq, err := http.NewRequest("POST", url, bytes.NewReader(reqBody))
	if err != nil {
		return BatchPushInfo{}, err
	}
	httpReq.Header.Add("Authorization", dataLayerAuth)
	httpResp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return BatchPushInfo{}, err
	}

	var info BatchPushInfo
	info.HTTPStatusCode = httpResp.StatusCode
	if httpResp.StatusCode == http.StatusOK {
		var resp BatchPushResponse
		if err := json.NewDecoder(httpResp.Body).Decode(&resp); err != nil {
			return info, fmt.Errorf("error decoding batch push response: %w", err)
		}
		info.BatchPushResponse = resp
	} else {
		body, err := ioutil.ReadAll(httpResp.Body)
		var s string
		if err == nil {
			s = string(body)
		} else {
			s = err.Error()
		}
		info.ErrorMessage = s
	}

	return info, nil
}
