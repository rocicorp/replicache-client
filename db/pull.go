package db

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"

	"roci.dev/diff-server/kv"
	servetypes "roci.dev/diff-server/serve/types"
	"roci.dev/diff-server/util/chk"
	"roci.dev/diff-server/util/countingreader"

	"github.com/attic-labs/noms/go/marshal"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/verbose"
	"github.com/pkg/errors"
)

type PullAuthError struct {
	error
}

type Progress func(bytesReceived, bytesExpected uint64)

func baseSnapshot(noms types.ValueReadWriter, c Commit) (Commit, error) {
	if c.Type() == CommitTypeSnapshot {
		return c, nil
	}
	basis, err := c.Basis(noms)
	if err != nil {
		return Commit{}, fmt.Errorf("could not find base snapshot of %v: %w", c.NomsStruct.Hash(), err)
	}
	return baseSnapshot(noms, basis)
}

const sandboxAuthorization = "sandbox"

// Pull pulls new server state from the client side.
// This function is doomed; the full implementation of sync will use pull() below.
func (db *DB) Pull(remote spec.Spec, clientViewAuth string, progress Progress) (servetypes.ClientViewInfo, error) {
	genesis, err := baseSnapshot(db.noms, db.head)
	if err != nil {
		return servetypes.ClientViewInfo{}, err
	}
	url := fmt.Sprintf("%s/pull", remote.String())
	pullReq, err := json.Marshal(servetypes.PullRequest{
		ClientViewAuth: clientViewAuth,
		ClientID:       db.clientID,
		BaseStateID:    genesis.Meta.Snapshot.ServerStateID,
		Checksum:       string(genesis.Value.Checksum),
	})
	verbose.Log("Pulling: %s from baseStateID %s", url, genesis.Meta.Snapshot.ServerStateID)
	verbose.Log("Pulling: clientViewAuth: %s", clientViewAuth)
	chk.NoError(err)

	req, err := http.NewRequest("POST", url, bytes.NewReader(pullReq))
	if err != nil {
		return servetypes.ClientViewInfo{}, err
	}
	req.Header.Add("Authorization", sandboxAuthorization) // TODO expose this in the constructor so clients can set it
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return servetypes.ClientViewInfo{}, err
	}

	if resp.StatusCode != http.StatusOK {
		body, err := ioutil.ReadAll(resp.Body)
		var s string
		if err == nil {
			s = string(body)
		} else {
			s = err.Error()
		}
		return servetypes.ClientViewInfo{}, fmt.Errorf("%s: %s", resp.Status, s)
	}

	getExpectedLength := func() (r int64, err error) {
		var s = resp.Header.Get("Entity-length")
		if s != "" {
			r, err = strconv.ParseInt(s, 10, 64)
			if err != nil {
				return 0, fmt.Errorf("Non-integral value for Entity-length header: %s", s)
			}
			return r, nil
		}
		if resp.ContentLength >= 0 {
			return resp.ContentLength, nil
		}
		return 0, nil
	}

	var pullResp servetypes.PullResponse
	var r io.Reader = resp.Body
	if progress != nil {
		cr := &countingreader.Reader{
			R: resp.Body,
		}
		expected, err := getExpectedLength()
		if err != nil {
			return servetypes.ClientViewInfo{}, err
		}
		cr.Callback = func() {
			rec := cr.Count
			exp := uint64(expected)
			if exp == 0 {
				exp = rec
			} else if rec > exp {
				rec = exp
			}
			progress(rec, exp)
		}
		r = cr
	}
	err = json.NewDecoder(r).Decode(&pullResp)
	if err != nil {
		return servetypes.ClientViewInfo{}, fmt.Errorf("Response from %s is not valid JSON: %s", url, err.Error())
	}

	if pullResp.LastMutationID < genesis.Meta.Snapshot.LastMutationID {
		return pullResp.ClientViewInfo, fmt.Errorf("Client view lastMutationID %d is < previous lastMutationID %d; ignoring", pullResp.LastMutationID, genesis.Meta.Snapshot.LastMutationID)
	}
	patchedMap, err := kv.ApplyPatch(db.Noms(), genesis.Data(db.noms), pullResp.Patch)
	if err != nil {
		return pullResp.ClientViewInfo, errors.Wrap(err, "couldnt apply patch")
	}
	expectedChecksum, err := kv.ChecksumFromString(pullResp.Checksum)
	if err != nil {
		return pullResp.ClientViewInfo, errors.Wrapf(err, "response checksum malformed: %s", pullResp.Checksum)
	}
	if patchedMap.Checksum() != expectedChecksum.String() {
		return pullResp.ClientViewInfo, fmt.Errorf("Checksum mismatch! Expected %s, got %s", expectedChecksum, patchedMap.Checksum())
	}
	newHead := makeSnapshot(db.noms, genesis.Ref(), pullResp.StateID, db.noms.WriteValue(patchedMap.NomsMap()), patchedMap.NomsChecksum(), pullResp.LastMutationID)
	db.noms.SetHead(db.noms.GetDataset(LOCAL_DATASET), db.noms.WriteValue(marshal.MustMarshal(db.noms, newHead)))

	return pullResp.ClientViewInfo, db.init()
}

type puller interface {
	Pull(noms types.ValueReadWriter, baseState Commit, url string, clientViewAuth string, clientID string) (Commit, servetypes.ClientViewInfo, error)
}

type defaultPuller struct{}

func (defaultPuller) Pull(noms types.ValueReadWriter, baseState Commit, url string, clientViewAuth string, clientID string) (Commit, servetypes.ClientViewInfo, error) {
	return pull(noms, baseState, url, clientViewAuth, clientID)
}

// Pull pulls new server state from the client view via the diffserver.
// TODO pass in auth (sandbox is hardcoded)
func pull(noms types.ValueReadWriter, baseState Commit, url string, clientViewAuth string, clientID string) (Commit, servetypes.ClientViewInfo, error) {
	baseMap := baseState.Data(noms)
	pullReq, err := json.Marshal(servetypes.PullRequest{
		ClientViewAuth: clientViewAuth,
		ClientID:       clientID,
		BaseStateID:    baseState.Meta.Snapshot.ServerStateID,
		Checksum:       baseMap.Checksum(),
	})
	if err != nil {
		return Commit{}, servetypes.ClientViewInfo{}, errors.New("could not marshal PullRequest")
	}
	verbose.Log("Pulling: %s from baseStateID %s with auth %s", url, baseState.Meta.Snapshot.ServerStateID, clientViewAuth)

	req, err := http.NewRequest("POST", url, bytes.NewReader(pullReq))
	if err != nil {
		return Commit{}, servetypes.ClientViewInfo{}, err
	}
	req.Header.Add("Authorization", sandboxAuthorization) // TODO expose this in the constructor so clients can set it
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return Commit{}, servetypes.ClientViewInfo{}, err
	}

	if resp.StatusCode != http.StatusOK {
		body, err := ioutil.ReadAll(resp.Body)
		var s string
		if err == nil {
			s = string(body)
		} else {
			s = err.Error()
		}
		return Commit{}, servetypes.ClientViewInfo{}, fmt.Errorf("%s: %s", resp.Status, s)
	}

	var pullResp servetypes.PullResponse
	var r io.Reader = resp.Body
	err = json.NewDecoder(r).Decode(&pullResp)
	if err != nil {
		return Commit{}, servetypes.ClientViewInfo{}, fmt.Errorf("response from %s is not valid JSON: %s", url, err.Error())
	}

	if pullResp.LastMutationID < baseState.Meta.Snapshot.LastMutationID {
		return Commit{}, pullResp.ClientViewInfo, fmt.Errorf("client view lastMutationID %d is < previous lastMutationID %d; ignoring", pullResp.LastMutationID, baseState.Meta.Snapshot.LastMutationID)
	}
	patchedMap, err := kv.ApplyPatch(noms, baseMap, pullResp.Patch)
	if err != nil {
		return Commit{}, pullResp.ClientViewInfo, errors.Wrap(err, "couldn't apply patch")
	}
	expectedChecksum, err := kv.ChecksumFromString(pullResp.Checksum)
	if err != nil {
		return Commit{}, pullResp.ClientViewInfo, errors.Wrapf(err, "response checksum malformed: %s", pullResp.Checksum)
	}
	if patchedMap.Checksum() != expectedChecksum.String() {
		return Commit{}, pullResp.ClientViewInfo, fmt.Errorf("checksum mismatch! Expected %s, got %s", expectedChecksum, patchedMap.Checksum())
	}
	newSnapshot := makeSnapshot(noms, baseState.Ref(), pullResp.StateID, noms.WriteValue(patchedMap.NomsMap()), patchedMap.NomsChecksum(), pullResp.LastMutationID)
	return newSnapshot, pullResp.ClientViewInfo, nil
}
