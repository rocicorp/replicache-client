// Package api implements the high-level API that is exposed to clients.
// Since we have many clients in many different languages, this is implemented
// language/host-indepedently, and further adapted by different packages.
package repm

import (
	"encoding/json"

	"roci.dev/replicache-client/db"

	jsnoms "roci.dev/diff-server/util/noms/json"
)

type GetRootRequest struct {
}

type GetRootResponse struct {
	Root jsnoms.Hash `json:"root"`
}

type HasRequest struct {
	transactionRequest
	Key string `json:"key"`
}

type HasResponse struct {
	Has bool `json:"has"`
}

type GetRequest struct {
	transactionRequest
	Key string `json:"key"`
}

type GetResponse struct {
	Has   bool            `json:"has"`
	Value json.RawMessage `json:"value,omitempty"`
}

type ScanRequest struct {
	transactionRequest
	db.ScanOptions
}

type ScanItem struct {
	Key   string       `json:"key"`
	Value jsnoms.Value `json:"value"`
}

type ScanResponse struct {
	Values []ScanItem `json:"values"`
	Done   bool       `json:"done"`
}

type PutRequest struct {
	transactionRequest
	Key   string          `json:"key"`
	Value json.RawMessage `json:"value"`
}

type PutResponse struct{}

type DelRequest struct {
	transactionRequest
	Key string `json:"key"`
}

type DelResponse struct {
	Ok bool `json:"ok"`
}

type PullRequest struct {
	Remote         jsnoms.Spec `json:"remote"`
	ClientViewAuth string      `json:"clientViewAuth"`
}

type PullResponseError struct {
	BadAuth string `json:"badAuth"`
}

type PullResponse struct {
	Error *PullResponseError `json:"error,omitempty"`
	Root  jsnoms.Hash        `json:"root,omitempty"`
}

type PullProgressRequest struct {
}

type PullProgressResponse struct {
	BytesReceived uint64 `json:"bytesReceived"`
	BytesExpected uint64 `json:"bytesExpected"`
}

type openTransactionRequest struct {
	Name string          `json:"name,omitempty"`
	Args json.RawMessage `json:"args,omitempty"`
}

type openTransactionResponse struct {
	TransactionID int `json:"transactionId"`
}

type transactionRequest struct {
	TransactionID int `json:"transactionId"`
}

type closeTransactionRequest transactionRequest

type closeTransactionResponse struct {
}

type commitTransactionRequest transactionRequest

type commitTransactionResponse struct {
	Ref *jsnoms.Hash `json:"ref,omitempty"`
}
