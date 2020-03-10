package repm

import (
	"encoding/json"
	"errors"
	"sync/atomic"

	"roci.dev/diff-server/util/chk"
	jsnoms "roci.dev/diff-server/util/noms/json"
	"roci.dev/replicache-client/db"
)

type connection struct {
	dir     string
	db      *db.DB
	sp      syncProgress
	syncing int32
}

type syncProgress struct {
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
		res.Value = jsnoms.New(conn.db.Noms(), v)
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
	req := PutRequest{
		Value: jsnoms.Make(conn.db.Noms(), nil),
	}
	err := json.Unmarshal(reqBytes, &req)
	if err != nil {
		return nil, err
	}
	if req.Value.Value == nil {
		return nil, errors.New("value field is required")
	}
	err = conn.db.Put(req.ID, req.Value.Value)
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

func (conn *connection) dispatchRequestSync(reqBytes []byte) ([]byte, error) {
	var req SyncRequest
	err := json.Unmarshal(reqBytes, &req)
	if err != nil {
		return nil, err
	}

	if !atomic.CompareAndSwapInt32(&conn.syncing, 0, 1) {
		return nil, errors.New("There is already a sync in progress")
	}

	defer chk.True(atomic.CompareAndSwapInt32(&conn.syncing, 1, 0), "UNEXPECTED STATE: Overlapping syncs somehow!")

	req.Remote.Options.Authorization = req.Auth

	res := SyncResponse{}
	err = conn.db.RequestSync(req.Remote.Spec, func(received, expected uint64) {
		conn.sp = syncProgress{
			bytesReceived: received,
			bytesExpected: expected,
		}
	})
	if _, ok := err.(db.SyncAuthError); ok {
		res.Error = &SyncResponseError{
			BadAuth: err.Error(),
		}
		err = nil
	}
	if err != nil {
		return nil, err
	}
	if res.Error == nil {
		res.Root = jsnoms.Hash{
			Hash: conn.db.Hash(),
		}
	}
	return mustMarshal(res), nil
}

func (conn *connection) dispatchSyncProgress(reqBytes []byte) ([]byte, error) {
	var req SyncProgressRequest
	err := json.Unmarshal(reqBytes, &req)
	if err != nil {
		return nil, err
	}
	res := SyncProgressResponse{
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
