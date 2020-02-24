// Package api implements the high-level API that is exposed to clients.
// Since we have many clients in many different languages, this is implemented
// language/host-indepedently, and further adapted by different packages.
package api

import (
	"encoding/json"
	"errors"
	"sync/atomic"

	"roci.dev/diff-server/util/chk"
	jsnoms "roci.dev/diff-server/util/noms/json"
	"roci.dev/replicache-client/db"
	"roci.dev/replicache-client/exec"
)

type API struct {
	db      *db.DB
	sp      syncProgress
	syncing int32
}

type syncProgress struct {
	bytesReceived uint64
	bytesExpected uint64
}

func New(db *db.DB) *API {
	return &API{db: db}
}

func (api *API) Dispatch(name string, req []byte) ([]byte, error) {
	switch name {
	case "getRoot":
		return api.dispatchGetRoot(req)
	case "has":
		return api.dispatchHas(req)
	case "get":
		return api.dispatchGet(req)
	case "scan":
		return api.dispatchScan(req)
	case "put":
		return api.dispatchPut(req)
	case "del":
		return api.dispatchDel(req)
	case "getBundle":
		return api.dispatchGetBundle(req)
	case "putBundle":
		return api.dispatchPutBundle(req)
	case "exec":
		return api.dispatchExec(req)
	case "requestSync":
		return api.dispatchRequestSync(req)
	case "syncProgress":
		return api.dispatchSyncProgress(req)
	}
	chk.Fail("Unsupported rpc name: %s", name)
	return nil, nil
}

func (api *API) dispatchGetRoot(reqBytes []byte) ([]byte, error) {
	var req GetRootRequest
	err := json.Unmarshal(reqBytes, &req)
	if err != nil {
		return nil, err
	}

	res := GetRootResponse{
		Root: jsnoms.Hash{
			Hash: api.db.Hash(),
		},
	}
	return mustMarshal(res), nil
}

func (api *API) dispatchHas(reqBytes []byte) ([]byte, error) {
	var req HasRequest
	err := json.Unmarshal(reqBytes, &req)
	if err != nil {
		return nil, err
	}
	ok, err := api.db.Has(req.ID)
	if err != nil {
		return nil, err
	}
	res := HasResponse{
		Has: ok,
	}
	return mustMarshal(res), nil
}

func (api *API) dispatchGet(reqBytes []byte) ([]byte, error) {
	var req GetRequest
	err := json.Unmarshal(reqBytes, &req)
	if err != nil {
		return nil, err
	}
	v, err := api.db.Get(req.ID)
	if err != nil {
		return nil, err
	}
	res := GetResponse{}
	if v == nil {
		res.Has = false
	} else {
		res.Has = true
		res.Value = jsnoms.New(api.db.Noms(), v)
	}
	return mustMarshal(res), nil
}

func (api *API) dispatchScan(reqBytes []byte) ([]byte, error) {
	var req ScanRequest
	err := json.Unmarshal(reqBytes, &req)
	if err != nil {
		return nil, err
	}
	items, err := api.db.Scan(exec.ScanOptions(req))
	if err != nil {
		return nil, err
	}
	return mustMarshal(items), nil
}

func (api *API) dispatchPut(reqBytes []byte) ([]byte, error) {
	req := PutRequest{
		Value: jsnoms.Make(api.db.Noms(), nil),
	}
	err := json.Unmarshal(reqBytes, &req)
	if err != nil {
		return nil, err
	}
	if req.Value.Value == nil {
		return nil, errors.New("value field is required")
	}
	err = api.db.Put(req.ID, req.Value.Value)
	if err != nil {
		return nil, err
	}
	res := PutResponse{
		Root: jsnoms.Hash{
			Hash: api.db.Hash(),
		},
	}
	return mustMarshal(res), nil
}

func (api *API) dispatchDel(reqBytes []byte) ([]byte, error) {
	req := DelRequest{}
	err := json.Unmarshal(reqBytes, &req)
	if err != nil {
		return nil, err
	}
	ok, err := api.db.Del(req.ID)
	if err != nil {
		return nil, err
	}
	res := DelResponse{
		Ok: ok,
		Root: jsnoms.Hash{
			Hash: api.db.Hash(),
		},
	}
	return mustMarshal(res), nil
}

func (api *API) dispatchGetBundle(reqBytes []byte) ([]byte, error) {
	var req GetBundleRequest
	err := json.Unmarshal(reqBytes, &req)
	if err != nil {
		return nil, err
	}
	return mustMarshal(GetBundleResponse{
		Code: string(api.db.Bundle()),
	}), nil
}

func (api *API) dispatchPutBundle(reqBytes []byte) ([]byte, error) {
	var req PutBundleRequest
	err := json.Unmarshal(reqBytes, &req)
	if err != nil {
		return nil, err
	}
	err = api.db.PutBundle([]byte(req.Code))
	if err != nil {
		return nil, errors.New(err.Error())
	}
	res := PutBundleResponse{
		Root: jsnoms.Hash{
			Hash: api.db.Hash(),
		},
	}
	return mustMarshal(res), nil
}

func (api *API) dispatchExec(reqBytes []byte) ([]byte, error) {
	req := ExecRequest{
		Args: jsnoms.List{
			Value: jsnoms.Make(api.db.Noms(), nil),
		},
	}
	err := json.Unmarshal(reqBytes, &req)
	if err != nil {
		return nil, err
	}
	output, err := api.db.Exec(req.Name, req.Args.List())
	if err != nil {
		return nil, err
	}
	res := ExecResponse{
		Root: jsnoms.Hash{
			Hash: api.db.Hash(),
		},
	}
	if output != nil {
		res.Result = jsnoms.New(api.db.Noms(), output)
	}
	return mustMarshal(res), nil
}

func (api *API) dispatchRequestSync(reqBytes []byte) ([]byte, error) {
	var req SyncRequest
	err := json.Unmarshal(reqBytes, &req)
	if err != nil {
		return nil, err
	}

	if !atomic.CompareAndSwapInt32(&api.syncing, 0, 1) {
		return nil, errors.New("There is already a sync in progress")
	}

	defer chk.True(atomic.CompareAndSwapInt32(&api.syncing, 1, 0), "UNEXPECTED STATE: Overlapping syncs somehow!")

	req.Remote.Options.Authorization = req.Auth

	res := SyncResponse{}
	err = api.db.RequestSync(req.Remote.Spec, func(received, expected uint64) {
		api.sp = syncProgress{
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
			Hash: api.db.Hash(),
		}
	}
	return mustMarshal(res), nil
}

func (api *API) dispatchSyncProgress(reqBytes []byte) ([]byte, error) {
	var req SyncProgressRequest
	err := json.Unmarshal(reqBytes, &req)
	if err != nil {
		return nil, err
	}
	res := SyncProgressResponse{
		BytesReceived: api.sp.bytesReceived,
		BytesExpected: api.sp.bytesExpected,
	}
	return mustMarshal(res), nil
}

func mustMarshal(thing interface{}) []byte {
	data, err := json.Marshal(thing)
	chk.NoError(err)
	return data
}
