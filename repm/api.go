package repm

import (
	"encoding/json"
	"errors"
	"net/http"
	"sync/atomic"

	"roci.dev/diff-server/util/chk"
	jsnoms "roci.dev/diff-server/util/noms/json"
	"roci.dev/replicache-client/db"
)

type connection struct {
	dir     string
	db      *db.DB
	sp      pullProgress
	pulling int32
}

type pullProgress struct {
	bytesReceived uint64
	bytesExpected uint64
}

func (conn *connection) dispatchGetRoot(reqBytes []byte) ([]byte, error) {
	var req GetRootRequest
	err := json.Unmarshal(reqBytes, &req)
	if err != nil {
		return nil, err
	}

	res := GetRootResponse{
		Root: jsnoms.Hash{
			Hash: conn.db.Hash(),
		},
	}
	return mustMarshal(res), nil
}

func (conn *connection) dispatchHas(reqBytes []byte) ([]byte, error) {
	var req HasRequest
	err := json.Unmarshal(reqBytes, &req)
	if err != nil {
		return nil, err
	}
	ok, err := conn.db.Has(req.ID)
	if err != nil {
		return nil, err
	}
	res := HasResponse{
		Has: ok,
	}
	return mustMarshal(res), nil
}

func (conn *connection) dispatchGet(reqBytes []byte) ([]byte, error) {
	var req GetRequest
	err := json.Unmarshal(reqBytes, &req)
	if err != nil {
		return nil, err
	}
	v, err := conn.db.Get(req.ID)
	if err != nil {
		return nil, err
	}
	res := GetResponse{}
	if v == nil {
		res.Has = false
	} else {
		res.Has = true
		res.Value = v
	}
	return mustMarshal(res), nil
}

func (conn *connection) dispatchScan(reqBytes []byte) ([]byte, error) {
	var req ScanRequest
	err := json.Unmarshal(reqBytes, &req)
	if err != nil {
		return nil, err
	}
	items, err := conn.db.Scan(db.ScanOptions(req))
	if err != nil {
		return nil, err
	}
	return mustMarshal(items), nil
}

func (conn *connection) dispatchPut(reqBytes []byte) ([]byte, error) {
	var req PutRequest
	err := json.Unmarshal(reqBytes, &req)
	if err != nil {
		return nil, err
	}
	if len(req.Value) == 0 {
		return nil, errors.New("value field is required")
	}
	err = conn.db.Put(req.ID, req.Value)
	if err != nil {
		return nil, err
	}
	res := PutResponse{
		Root: jsnoms.Hash{
			Hash: conn.db.Hash(),
		},
	}
	return mustMarshal(res), nil
}

func (conn *connection) dispatchDel(reqBytes []byte) ([]byte, error) {
	req := DelRequest{}
	err := json.Unmarshal(reqBytes, &req)
	if err != nil {
		return nil, err
	}
	ok, err := conn.db.Del(req.ID)
	if err != nil {
		return nil, err
	}
	res := DelResponse{
		Ok: ok,
		Root: jsnoms.Hash{
			Hash: conn.db.Hash(),
		},
	}
	return mustMarshal(res), nil
}

func (conn *connection) dispatchPull(reqBytes []byte) ([]byte, error) {
	var req PullRequest
	err := json.Unmarshal(reqBytes, &req)
	if err != nil {
		return nil, err
	}

	if !atomic.CompareAndSwapInt32(&conn.pulling, 0, 1) {
		return nil, errors.New("There is already a pull in progress")
	}

	defer chk.True(atomic.CompareAndSwapInt32(&conn.pulling, 1, 0), "UNEXPECTED STATE: Overlapping pulls somehow!")

	res := PullResponse{}
	clientViewInfo, err := conn.db.Pull(req.Remote.Spec, req.ClientViewAuth, func(received, expected uint64) {
		conn.sp = pullProgress{
			bytesReceived: received,
			bytesExpected: expected,
		}
	})
	if err != nil {
		return nil, err
	}
	res.Root = jsnoms.Hash{
		Hash: conn.db.Hash(),
	}

	if clientViewInfo.HTTPStatusCode == http.StatusUnauthorized {
		res.Error = &PullResponseError{
			BadAuth: clientViewInfo.ErrorMessage,
		}
	}

	return mustMarshal(res), nil
}

func (conn *connection) dispatchPullProgress(reqBytes []byte) ([]byte, error) {
	var req PullProgressRequest
	err := json.Unmarshal(reqBytes, &req)
	if err != nil {
		return nil, err
	}
	res := PullProgressResponse{
		BytesReceived: conn.sp.bytesReceived,
		BytesExpected: conn.sp.bytesExpected,
	}
	return mustMarshal(res), nil
}

func mustMarshal(thing interface{}) []byte {
	data, err := json.Marshal(thing)
	chk.NoError(err)
	return data
}
