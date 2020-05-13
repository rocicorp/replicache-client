package db

import (
	"errors"
	"fmt"
	"testing"

	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/datetime"
	"github.com/stretchr/testify/assert"
	assertpkg "github.com/stretchr/testify/assert"
	"roci.dev/diff-server/kv"
	servetypes "roci.dev/diff-server/serve/types"
	"roci.dev/diff-server/util/log"
	nomsjson "roci.dev/diff-server/util/noms/json"
)

func TestDB_BeginSync(t *testing.T) {
	assert := assertpkg.New(t)
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

		// Pull
		pullCVI servetypes.ClientViewInfo
		pullErr string

		// BeginSync
		wantSyncHead hash.Hash
		wantCVI      servetypes.ClientViewInfo
		wantBPI      *BatchPushInfo
		wantErr      string
	}{
		{
			"good push, good pull",
			2,
			[]uint64{1, 2},
			BatchPushInfo{HTTPStatusCode: 1},
			servetypes.ClientViewInfo{HTTPStatusCode: 2},
			"",
			syncSnapshot.NomsStruct.Hash(),
			servetypes.ClientViewInfo{HTTPStatusCode: 2},
			&BatchPushInfo{HTTPStatusCode: 1},
			"",
		},
		{
			"no push, good pull",
			0,
			[]uint64{},
			BatchPushInfo{},
			servetypes.ClientViewInfo{HTTPStatusCode: 2},
			"",
			syncSnapshot.NomsStruct.Hash(),
			servetypes.ClientViewInfo{HTTPStatusCode: 2},
			nil,
			"",
		},
		{
			"push errors, good pull",
			1,
			[]uint64{1},
			BatchPushInfo{ErrorMessage: "push error"},
			servetypes.ClientViewInfo{HTTPStatusCode: 2},
			"",
			syncSnapshot.NomsStruct.Hash(),
			servetypes.ClientViewInfo{HTTPStatusCode: 2},
			&BatchPushInfo{ErrorMessage: "push error"},
			"",
		},
		{
			"good push, pull errors",
			1,
			[]uint64{1},
			BatchPushInfo{HTTPStatusCode: 1},
			servetypes.ClientViewInfo{},
			"pull error",
			hash.Hash{},
			servetypes.ClientViewInfo{},
			&BatchPushInfo{HTTPStatusCode: 1},
			"pull error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert = assertpkg.New(t)
			db, dir := LoadTempDB(assert)
			fmt.Println("dir", dir)

			var commits testCommits
			commits.addGenesis(assert, db)
			for i := 0; i < tt.numLocals; i++ {
				commits.addLocal(assert, db, d)
			}
			assert.NoError(db.setHead(commits.head()))

			syncSnapshot = makeSnapshot(db.noms, commits.genesis().Ref(), "newssid", db.Noms().WriteValue(m.NomsMap()), m.NomsChecksum(), 43)
			// Ensure it is not saved so we can check that it is by sync.
			assert.Nil(db.noms.ReadValue(syncSnapshot.NomsStruct.Hash()))

			fakePusher := fakePusher{
				info: tt.pushInfo,
			}
			db.pusher = &fakePusher
			fakePuller := fakePuller{
				newSnapshot:    syncSnapshot,
				clientViewInfo: tt.pullCVI,
				err:            tt.pullErr,
			}
			db.puller = &fakePuller

			gotSyncHead, gotSyncInfo, gotErr := db.BeginSync(batchPushURL, diffServerURL, dataLayerAuth, log.Default())
			// Push-specific assertions.
			if tt.numLocals > 0 {
				assert.Equal(batchPushURL, fakePusher.gotURL)
				assert.Equal(dataLayerAuth, fakePusher.gotDataLayerAuth)
				assert.Equal(db.clientID, fakePusher.gotObfuscatedClientID)
				var gotMutationIDs []uint64
				for _, m := range fakePusher.gotPending {
					gotMutationIDs = append(gotMutationIDs, m.MutationID)
				}
				assert.Equal(tt.wantPushMutationIDs, gotMutationIDs)
			}

			// Pull-specific assertions.
			assert.True(commits.genesis().NomsStruct.Equals(fakePuller.gotBaseState.NomsStruct))
			assert.Equal(diffServerURL, fakePuller.gotURL)
			assert.Equal(dataLayerAuth, fakePuller.gotClientViewAuth)

			// BeginSync behavior as a whole.
			assert.Equal(tt.wantSyncHead, gotSyncHead)
			assert.Equal(tt.wantCVI, gotSyncInfo.ClientViewInfo)
			assert.Equal(tt.wantBPI, gotSyncInfo.BatchPushInfo)
			assert.NoError(db.Reload())
			assert.True(commits.head().NomsStruct.Equals(db.Head().NomsStruct))
			if tt.wantErr != "" {
				assert.Error(gotErr)
				assert.Regexp(tt.wantErr, gotErr.Error())
				assert.Nil(db.noms.ReadValue(syncSnapshot.NomsStruct.Hash()))

			} else {
				assert.NoError(gotErr)
				assert.NotNil(db.noms.ReadValue(syncSnapshot.NomsStruct.Hash()))
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
}

func (f *fakePusher) Push(pending []Local, url string, dataLayerAuth string, obfuscatedClientID string) BatchPushInfo {
	f.gotPending = pending
	f.gotURL = url
	f.gotDataLayerAuth = dataLayerAuth
	f.gotObfuscatedClientID = obfuscatedClientID
	return f.info
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
		expReplayIds     []uint64
		expErr           string
	}{
		{
			"nothing pending",
			0,
			0,
			false,
			[]uint64{},
			"",
		},
		{
			"2 pending but nothing to replay",
			2,
			0,
			false,
			[]uint64{},
			"",
		},
		{
			"3 pending, 2 to replay",
			3,
			2,
			false,
			[]uint64{2, 3},
			"",
		},
		{
			"a different sync has landed a new snapshot on master with no pending",
			0,
			0,
			true,
			[]uint64{},
			"newer snapshot",
		},
		{
			"a different sync has landed a new snapshot on master with pending",
			2,
			0,
			true,
			[]uint64{},
			"newer snapshot",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, _ := LoadTempDB(assert)
			var master testCommits
			master = append(master, db.Head())
			for i := 0; i < tt.numPending; i++ {
				master.addLocal(assert, db, d)
			}
			if tt.interveningSync {
				master.addSnapshot(assert, db)
			}
			assert.NoError(db.setHead(master.head()))

			syncBranch := testCommits{master.genesis()}
			syncBranch.addSnapshot(assert, db)
			// Add the already replayed mutations to the sync branch.
			for i := 0; i < tt.numPending-tt.numNeedingReplay; i++ {
				// Pending commits start at index 1 (index 0 is genesis).
				masterIndex := 1 + i
				original := master[masterIndex]
				assert.True(original.Type() == CommitTypeLocal)
				replayed := makeLocal(db.noms, syncBranch.head().Ref(), d, original.MutationID(), original.Meta.Local.Name, original.Meta.Local.Args, original.Value.Data, original.Value.Checksum)
				db.noms.WriteValue(replayed.NomsStruct)
				syncBranch = append(syncBranch, replayed)
			}
			syncHead := syncBranch.head()

			gotReplay, err := db.MaybeEndSync(syncHead.NomsStruct.Hash())

			if tt.expErr != "" {
				assert.Error(err)
				if err != nil {
					assert.Regexp(tt.expErr, err.Error())
				}
				assert.Equal(0, len(gotReplay))
			} else {
				assert.NoError(err)
				assert.Equal(len(tt.expReplayIds), len(gotReplay))
				if len(tt.expReplayIds) == len(gotReplay) {
					for i, mutationID := range tt.expReplayIds {
						assert.True(int(mutationID) < len(master))
						assert.Equal(master[mutationID].Meta.Local.Name, gotReplay[i].Name)
						gotArgs, err := nomsjson.FromJSON(gotReplay[i].Args, db.noms)
						assert.NoError(err)
						assert.True(master[mutationID].Meta.Local.Args.Equals(gotArgs))
						assert.Equal(master[mutationID].Ref().TargetHash(), gotReplay[i].Original.Hash)
					}
				}
			}
			// If successful...
			if tt.expErr == "" && len(tt.expReplayIds) == 0 {
				assert.True(syncHead.NomsStruct.Equals(db.Head().NomsStruct))
			} else {
				assert.True(master.head().NomsStruct.Equals(db.Head().NomsStruct))
			}
		})
	}
}

func TestPendingCommits(t *testing.T) {
	assert := assert.New(t)

	db, _ := LoadTempDB(assert)
	commits := &testCommits{}

	f := func(head Commit, wantCommits []Commit, wantErr error) {
		gotCommits, gotErr := pendingCommits(db.Noms(), head)
		if wantErr != nil {
			assert.Nil(gotCommits)
			assert.Equal(wantErr, gotErr)
		} else {
			assert.Equal(len(wantCommits), len(gotCommits))
			for i := range wantCommits {
				assert.True(wantCommits[i].NomsStruct.Equals(gotCommits[i].NomsStruct))
			}
			assert.NoError(gotErr)
		}
	}

	f(commits.addGenesis(assert, db).head(), []Commit{}, nil)
	f(commits.addSnapshot(assert, db).head(), []Commit{}, nil)
	f(commits.addLocal(assert, db, datetime.Now()).head(), []Commit{commits.head()}, nil)
	f(commits.addSnapshot(assert, db).head(), []Commit{}, nil)
	f(commits.addLocal(assert, db, datetime.Now()).head(), []Commit{commits.head()}, nil)
	f(commits.addLocal(assert, db, datetime.Now()).head(), []Commit((*commits)[len(*commits)-2:]), nil)
	f(commits.addSnapshot(assert, db).head(), []Commit{}, nil)
}
