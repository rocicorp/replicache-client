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

func findGenesis(noms types.ValueReadWriter, c Commit) (Commit, error) {
	if c.Type() == CommitTypeGenesis {
		return c, nil
	}

	for p := c; len(p.Parents) > 0; {
		v := noms.ReadValue(p.Parents[0].Hash())
		if v == nil {
			return Commit{}, fmt.Errorf("could not find parent %v", p.Parents[0])
		} else {
			err := marshal.Unmarshal(v, &p)
			if err != nil {
				return Commit{}, fmt.Errorf("Error: Parent is not a commit: %#v", v)
			}
		}
		if p.Type() == CommitTypeGenesis {
			return p, nil
		}
	}

	return Commit{}, fmt.Errorf("could not find genesis of %v", c)
}

const sandboxAccountID = "sandbox"
const clientViewUserID = "42"
const fakeClientID = "fake-client-id"

// RequestSync pulls new server state from the client side.
func (db *DB) RequestSync(remote spec.Spec, progress Progress) error {
	genesis, err := findGenesis(db.noms, db.head)
	if err != nil {
		return err
	}
	url := fmt.Sprintf("%s/pull", remote.String())
	// TODO test walking backwards works
	pullReq, err := json.Marshal(servetypes.PullRequest{
		ClientViewAuth: clientViewUserID, // TODO hook this up to the client so it can specify
		ClientID:       fakeClientID,     // TODO hook this up to the client id generation code
		BaseStateID:    genesis.Meta.Genesis.ServerStateID,
		Checksum:       string(genesis.Value.Checksum),
	})
	verbose.Log("Pulling: %s from baseStateID %s", url, genesis.Meta.Genesis.ServerStateID)
	chk.NoError(err)

	req, err := http.NewRequest("POST", url, bytes.NewReader(pullReq))
	if err != nil {
		return err
	}
	req.Header.Add("Authorization", sandboxAccountID) // TODO expose this in the constructor so clients can set it
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		body, err := ioutil.ReadAll(resp.Body)
		var s string
		if err == nil {
			s = string(body)
		} else {
			s = err.Error()
		}
		err = fmt.Errorf("%s: %s", resp.Status, s)
		if resp.StatusCode == http.StatusForbidden {
			return PullAuthError{
				err,
			}
		} else {
			return err
		}
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
			return err
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
		return fmt.Errorf("Response from %s is not valid JSON: %s", url, err.Error())
	}

	patchedMap, err := kv.ApplyPatch(kv.NewMapFromNoms(db.noms, genesis.Data(db.noms)), pullResp.Patch)
	if err != nil {
		return errors.Wrap(err, "couldnt apply patch")
	}
	expectedChecksum, err := kv.ChecksumFromString(pullResp.Checksum)
	if err != nil {
		return errors.Wrapf(err, "response checksum malformed: %s", pullResp.Checksum)
	}
	if !patchedMap.Checksum().Equal(*expectedChecksum) {
		return fmt.Errorf("Checksum mismatch! Expected %s, got %s", expectedChecksum.String(), patchedMap.Checksum().String())
	}
	newHead := makeGenesis(db.noms, pullResp.StateID, db.noms.WriteValue(patchedMap.NomsMap()), types.String(patchedMap.Checksum().String()), pullResp.LastMutationID)
	db.noms.SetHead(db.noms.GetDataset(LOCAL_DATASET), db.noms.WriteValue(marshal.MustMarshal(db.noms, newHead)))
	return db.init()
}
