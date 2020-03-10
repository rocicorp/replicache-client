// Package api implements the high-level API that is exposed to clients.
// Since we have many clients in many different languages, this is implemented
// language/host-indepedently, and further adapted by different packages.
package repm

import (
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
	Has   bool          `json:"has"`
	Value *jsnoms.Value `json:"value,omitempty"`
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
	ID    string       `json:"id"`
	Value jsnoms.Value `json:"value"`
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

type GetBundleRequest struct {
}

type GetBundleResponse struct {
	Code string `json:"code"`
}

type PutBundleRequest struct {
	Code string `json:"code"`
}

type PutBundleResponse struct {
	Root jsnoms.Hash `json:"root"`
}

type SyncRequest struct {
	Remote jsnoms.Spec `json:"remote"`
	Auth   string      `json:"auth,omitempty"`

	// Shallow causes only the head of the remote server to be downloaded, not all of its history.
	// Currently this is incompatible with bidirectional sync.
	Shallow bool `json:"shallow,omitempty"`
}

type SyncResponseError struct {
	BadAuth string `json:"badAuth,omitempty"`
}

type SyncResponse struct {
	Error *SyncResponseError `json:"error,omitempty"`
	Root  jsnoms.Hash        `json:"root,omitempty"`
}

type SyncProgressRequest struct {
}

type SyncProgressResponse struct {
	BytesReceived uint64 `json:"bytesReceived"`
	BytesExpected uint64 `json:"bytesExpected"`
}
