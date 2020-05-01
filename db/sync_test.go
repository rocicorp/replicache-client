package db

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/datetime"
	"github.com/stretchr/testify/assert"
	"roci.dev/diff-server/kv"
	servetypes "roci.dev/diff-server/serve/types"
	nomsjson "roci.dev/diff-server/util/noms/json"
)

func TestDB_BeginSync(t *testing.T) {
	assert := assert.New(t)
	d := datetime.Now()
	batchPushURL := "https://example.com/push"
	diffServerURL := "https://example.com/pull"
	dataLayerAuth := "dataLayerAuthToken"

	// Get the sync snapshot we expect in the success case and then throw away its db.
	db, _ := LoadTempDB(assert)
	m := kv.NewMap(db.noms)
	var commits testCommits
	commits.addGenesis(assert, db).addLocal(assert, db, d)
	syncSnapshot := makeSnapshot(db.noms, commits.genesis().Ref(), "newssid", db.Noms().WriteValue(m.NomsMap()), m.NomsChecksum(), 43)

	tests := []struct {
		name string

		// Push
		numLocals           int
		wantPushMutationIDs []uint64
		pushInfo            BatchPushInfo
		pushErr             string

		// Pull
		pullCVI servetypes.ClientViewInfo
		pullErr string

		// BeginSync
		wantSyncHead hash.Hash
		wantCVI      servetypes.ClientViewInfo
		wantBPI      BatchPushInfo
		wantErr      string
	}{
		{
			"good push, good pull",
			2,
			[]uint64{1, 2},
			BatchPushInfo{HTTPStatusCode: 1},
			"",
			servetypes.ClientViewInfo{HTTPStatusCode: 2},
			"",
			syncSnapshot.Original.Hash(),
			servetypes.ClientViewInfo{HTTPStatusCode: 2},
			BatchPushInfo{HTTPStatusCode: 1},
			"",
		},
		{
			"no push, good pull",
			0,
			[]uint64{},
			BatchPushInfo{},
			"",
			servetypes.ClientViewInfo{HTTPStatusCode: 2},
			"",
			syncSnapshot.Original.Hash(),
			servetypes.ClientViewInfo{HTTPStatusCode: 2},
			BatchPushInfo{},
			"",
		},
		{
			"push errors, good pull",
			1,
			[]uint64{1},
			BatchPushInfo{},
			"push error",
			servetypes.ClientViewInfo{HTTPStatusCode: 2},
			"",
			syncSnapshot.Original.Hash(),
			servetypes.ClientViewInfo{HTTPStatusCode: 2},
			BatchPushInfo{HTTPStatusCode: 1},
			"",
		},
		{
			"good push, pull errors",
			1,
			[]uint64{1},
			BatchPushInfo{HTTPStatusCode: 1},
			"",
			servetypes.ClientViewInfo{},
			"pull error",
			hash.Hash{},
			servetypes.ClientViewInfo{},
			BatchPushInfo{HTTPStatusCode: 1},
			"pull error",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, dir := LoadTempDB(assert)
			fmt.Println("dir", dir)

			var commits testCommits
			commits.addGenesis(assert, db)
			for i := 0; i < tt.numLocals; i++ {
				commits.addLocal(assert, db, d)
			}
			db.head = commits.head()
			_, err := db.noms.FastForward(db.noms.GetDataset(LOCAL_DATASET), db.head.Ref())
			assert.NoError(err)

			syncSnapshot = makeSnapshot(db.noms, commits.genesis().Ref(), "newssid", db.Noms().WriteValue(m.NomsMap()), m.NomsChecksum(), 43)
			// Ensure it is not saved so we can check that it is by sync.
			assert.Nil(db.noms.ReadValue(syncSnapshot.Original.Hash()))

			fakePusher := fakePusher{
				info: tt.wantBPI,
				err:  tt.pushErr,
			}
			db.pusher = &fakePusher
			fakePuller := fakePuller{
				newSnapshot:    syncSnapshot,
				clientViewInfo: tt.pullCVI,
				err:            tt.pullErr,
			}
			db.puller = &fakePuller

			gotSyncHead, gotSyncInfo, gotErr := db.BeginSync(batchPushURL, diffServerURL, dataLayerAuth)
			// Push-specific assertions.
			if tt.numLocals > 0 {
				assert.Equal(batchPushURL, fakePusher.gotURL, tt.name)
				assert.Equal(dataLayerAuth, fakePusher.gotDataLayerAuth)
				assert.Equal(db.clientID, fakePusher.gotObfuscatedClientID)
				var gotMutationIDs []uint64
				for _, m := range fakePusher.gotPending {
					gotMutationIDs = append(gotMutationIDs, m.MutationID)
				}
				assert.Equal(tt.wantPushMutationIDs, gotMutationIDs, tt.name)
			}

			// Pull-specific assertions.
			assert.True(commits.genesis().Original.Equals(fakePuller.gotBaseState.Original))
			assert.Equal(diffServerURL, fakePuller.gotURL)
			assert.Equal(dataLayerAuth, fakePuller.gotClientViewAuth)

			// BeginSync behavior as a whole.
			assert.Equal(tt.wantSyncHead, gotSyncHead)
			assert.Equal(tt.wantCVI, gotSyncInfo.ClientViewInfo)
			assert.Equal(tt.wantBPI, gotSyncInfo.BatchPushInfo)
			assert.NoError(db.Reload())
			assert.True(commits.head().Original.Equals(db.Head().Original))
			if tt.wantErr != "" {
				assert.Error(gotErr)
				assert.Regexp(tt.wantErr, gotErr.Error(), tt.name)
				assert.Nil(db.noms.ReadValue(syncSnapshot.Original.Hash()))

			} else {
				assert.NoError(gotErr)
				assert.NotNil(db.noms.ReadValue(syncSnapshot.Original.Hash()))
			}
		})
	}
}

type fakePusher struct {
	gotPending            []Local
	gotURL                string
	gotDataLayerAuth      string
	gotObfuscatedClientID string

	info BatchPushInfo
	err  string
}

func (f *fakePusher) Push(pending []Local, url string, dataLayerAuth string, obfuscatedClientID string) (BatchPushInfo, error) {
	f.gotPending = pending
	f.gotURL = url
	f.gotDataLayerAuth = dataLayerAuth
	f.gotObfuscatedClientID = obfuscatedClientID

	if f.err != "" {
		return f.info, errors.New(f.err)
	}
	return f.info, nil
}

type fakePuller struct {
	gotBaseState      Commit
	gotURL            string
	gotClientViewAuth string
	gotClientID       string

	newSnapshot    Commit
	clientViewInfo servetypes.ClientViewInfo
	err            string
}

func (f *fakePuller) Pull(noms types.ValueReadWriter, baseState Commit, url string, clientViewAuth string, clientID string) (Commit, servetypes.ClientViewInfo, error) {
	f.gotBaseState = baseState
	f.gotURL = url
	f.gotClientViewAuth = clientViewAuth
	f.gotClientID = clientID

	if f.err == "" {
		return f.newSnapshot, f.clientViewInfo, nil
	}
	return Commit{}, f.clientViewInfo, errors.New(f.err)
}

func TestDB_MaybeEndSync(t *testing.T) {
	assert := assert.New(t)
	d := datetime.Now()

	tests := []struct {
		name             string
		numPending       int
		numNeedingReplay int
		interveningSync  bool
		expEnded         bool
		expReplayIds     []uint64
		expErr           string
	}{
		{
			"nothing pending",
			0,
			0,
			false,
			true,
			[]uint64{},
			"",
		},
		{
			"2 pending but nothing to replay",
			2,
			0,
			false,
			true,
			[]uint64{},
			"",
		},
		{
			"3 pending, 2 to replay",
			3,
			2,
			false,
			false,
			[]uint64{2, 3},
			"",
		},
		{
			"a different sync has landed a new snapshot on master with no pending",
			0,
			0,
			true,
			false,
			[]uint64{},
			"newer snapshot",
		},
		{
			"a different sync has landed a new snapshot on master with pending",
			2,
			0,
			true,
			false,
			[]uint64{},
			"newer snapshot",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, _ := LoadTempDB(assert)
			var master testCommits
			master = append(master, db.head)
			for i := 0; i < tt.numPending; i++ {
				master.addLocal(assert, db, d)
			}
			if tt.interveningSync {
				master.addSnapshot(assert, db)
			}
			db.head = master.head()
			_, err := db.noms.FastForward(db.noms.GetDataset(LOCAL_DATASET), db.head.Ref())
			assert.NoError(err)

			syncBranch := testCommits{master.genesis()}
			syncBranch.addSnapshot(assert, db)
			// Add the already replayed mutations to the sync branch.
			for i := 0; i < tt.numPending-tt.numNeedingReplay; i++ {
				// Pending commits start at index 1 (index 0 is genesis).
				masterIndex := 1 + i
				original := master[masterIndex]
				assert.True(original.Type() == CommitTypeLocal)
				replayed := makeLocal(db.noms, syncBranch.head().Ref(), d, original.MutationID(), original.Meta.Local.Name, original.Meta.Local.Args, original.Value.Data, original.Value.Checksum)
				db.noms.WriteValue(replayed.Original)
				syncBranch = append(syncBranch, replayed)
			}
			syncHead := syncBranch.head()

			gotEnded, gotReplay, err := db.MaybeEndSync(syncHead.Original.Hash())

			assert.Equal(tt.expEnded, gotEnded, tt.name)
			if tt.expErr != "" {
				assert.Error(err)
				if err != nil {
					assert.Regexp(tt.expErr, err.Error(), tt.name)
				}
				assert.False(gotEnded)
			} else {
				assert.NoError(err, tt.name)
				assert.Equal(len(tt.expReplayIds), len(gotReplay), tt.name)
				if len(tt.expReplayIds) == len(gotReplay) {
					for i, mutationID := range tt.expReplayIds {
						assert.True(int(mutationID) < len(master))
						assert.Equal(master[mutationID].Meta.Local.Name, gotReplay[i].Name)
						gotArgs, err := nomsjson.FromJSON(bytes.NewReader(gotReplay[i].Args), db.noms)
						assert.NoError(err)
						assert.True(master[mutationID].Meta.Local.Args.Equals(gotArgs))
					}
				}
			}
			if tt.expEnded {
				assert.True(syncHead.Original.Equals(db.head.Original))
			} else {
				assert.True(master.head().Original.Equals(db.head.Original))
			}
		})
	}
}
