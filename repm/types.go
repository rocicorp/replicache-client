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
	ID string `json:"id"`
}

type HasResponse struct {
	Has bool `json:"has"`
}

type GetRequest struct {
	ID string `json:"id"`
}

type GetResponse struct {
	Has   bool            `json:"has"`
	Value json.RawMessage `json:"value,omitempty"`
}

type ScanRequest db.ScanOptions

type ScanItem struct {
	ID    string       `json:"id"`
	Value jsnoms.Value `json:"value"`
}

type ScanResponse struct {
	Values []ScanItem `json:"values"`
	Done   bool       `json:"done"`
}

type PutRequest struct {
	ID    string          `json:"id"`
	Value json.RawMessage `json:"value"`
}

type PutResponse struct {
	Root jsnoms.Hash `json:"root"`
}

type DelRequest struct {
	ID string `json:"id"`
}

type DelResponse struct {
	Ok   bool        `json:"ok"`
	Root jsnoms.Hash `json:"root"`
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
