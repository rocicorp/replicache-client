package db

import (
	"bytes"
	"fmt"
	"log"

	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/marshal"
	servetypes "roci.dev/diff-server/serve/types"
	nomsjson "roci.dev/diff-server/util/noms/json"
)

type SyncInfo struct {
	BatchPushInfo  BatchPushInfo             `json:"batchPushInfo"`
	ClientViewInfo servetypes.ClientViewInfo `json:"clientViewInfo"`
}

// BeginSync pushes pending mutations to the data layer and pulls new state via the client view.
func (db *DB) BeginSync(batchPushURL string, diffServerURL string, dataLayerAuth string) (hash.Hash, SyncInfo, error) {
	syncInfo := SyncInfo{}
	head := db.Head()

	// Push
	pendingCommits, err := pendingCommits(db.noms, head)
	if err != nil {
		return hash.Hash{}, syncInfo, err
	}
	if len(pendingCommits) > 0 {
		var mutations []Local
		for _, c := range pendingCommits {
			mutations = append(mutations, c.Meta.Local)
		}
		// TODO use obfuscated client ID
		batchPushInfo, err := db.pusher.Push(mutations, batchPushURL, dataLayerAuth, db.clientID)
		if err != nil {
			log.Printf("batch push failed: %s; continuing with sync", err)
			// Note: on error we continue, not return.
		}
		syncInfo.BatchPushInfo = batchPushInfo
	}

	// Pull
	headSnapshot, err := baseSnapshot(db.noms, head)
	if err != nil {
		return hash.Hash{}, syncInfo, fmt.Errorf("sync failed: could not find head snapshot: %w", err)
	}
	newSnapshot, clientViewInfo, err := db.puller.Pull(db.noms, headSnapshot, diffServerURL, dataLayerAuth, db.clientID)
	syncInfo.ClientViewInfo = clientViewInfo
	if err != nil {
		return hash.Hash{}, syncInfo, fmt.Errorf("sync failed: %w", err)
	}
	syncHeadRef := db.noms.WriteValue(newSnapshot.NomsStruct)

	return syncHeadRef.TargetHash(), syncInfo, nil
}

func (db *DB) MaybeEndSync(syncHead hash.Hash) (bool, []Mutation, error) {
	v := db.Noms().ReadValue(syncHead)
	if v == nil {
		return false, []Mutation{}, fmt.Errorf("could not load sync head %s", syncHead)
	}
	var syncHeadCommit Commit
	if err := marshal.Unmarshal(v, &syncHeadCommit); err != nil {
		return false, []Mutation{}, err
	}

	defer db.lock()()
	head := db.head

	// Stop if someone landed a sync since this sync started.
	syncSnapshot, err := baseSnapshot(db.noms, syncHeadCommit)
	if err != nil {
		return false, []Mutation{}, err
	}
	syncSnapshotBasis, err := syncSnapshot.Basis(db.noms)
	if err != nil {
		return false, []Mutation{}, err
	}
	headSnapshot, err := baseSnapshot(db.noms, head)
	if err != nil {
		return false, []Mutation{}, err
	}
	if !syncSnapshotBasis.NomsStruct.Equals(headSnapshot.NomsStruct) {
		return false, []Mutation{}, fmt.Errorf("sync aborted: found a newer snapshot %s on master", headSnapshot.NomsStruct.Hash())
	}

	// Determine if there are any pending mutations that we need to replay.
	pendingCommits, err := pendingCommits(db.noms, head)
	if err != nil {
		return false, []Mutation{}, err
	}
	commitsToReplay := filterIDsLessThanOrEqualTo(pendingCommits, syncHeadCommit.MutationID())
	var replay []Mutation
	if len(commitsToReplay) > 0 {
		for _, c := range commitsToReplay {
			var args bytes.Buffer
			err = nomsjson.ToJSON(c.Meta.Local.Args, &args)
			if err != nil {
				return false, []Mutation{}, err
			}
			replay = append(replay, Mutation{
				ID:   c.MutationID(),
				Name: string(c.Meta.Local.Name),
				Args: args.Bytes(),
			})
		}
		return false, replay, nil
	}

	// TODO check invariants from synchead back to syncsnapshot.

	// Sync is complete. Can't ffwd because sync head is dangling.
	_, err = db.noms.SetHead(db.noms.GetDataset(MASTER_DATASET), syncHeadCommit.Ref())
	if err != nil {
		return false, []Mutation{}, err
	}
	db.head = syncHeadCommit

	return true, []Mutation{}, nil
}

// Assumes commits are in ascending order of mutation id.
func filterIDsLessThanOrEqualTo(commits []Commit, filter uint64) (filtered []Commit) {
	for i := 0; i < len(commits); i++ {
		if commits[i].MutationID() > filter {
			filtered = append(filtered, commits[i])
		}
	}
	return
}
