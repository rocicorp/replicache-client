package db

import (
	"bytes"
	"fmt"
	"log"

	"github.com/attic-labs/noms/go/hash"
	servetypes "roci.dev/diff-server/serve/types"
	nomsjson "roci.dev/diff-server/util/noms/json"
)

type SyncInfo struct {
	BatchPushInfo  *BatchPushInfo            `json:"batchPushInfo,omitempty"`
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
			log.Printf("DEBUG: unable to make request to %s: %s; continuing with sync", diffServerURL, err)
			// Note: on error we continue, not return.
		} else {
			log.Printf("DEBUG: batch push finished with status %d error message '%s'; continuing sync", batchPushInfo.HTTPStatusCode, batchPushInfo.ErrorMessage)
			syncInfo.BatchPushInfo = &batchPushInfo
		}
	}

	// Pull
	headSnapshot, err := baseSnapshot(db.noms, head)
	if err != nil {
		return hash.Hash{}, syncInfo, fmt.Errorf("sync failed: could not find head snapshot: %w", err)
	}
	newSnapshot, clientViewInfo, err := db.puller.Pull(db.noms, headSnapshot, diffServerURL, dataLayerAuth, db.clientID)
	if err != nil {
		return hash.Hash{}, syncInfo, fmt.Errorf("sync failed: %w", err)
	} else {
		syncInfo.ClientViewInfo = clientViewInfo
	}
	syncHeadRef := db.noms.WriteValue(newSnapshot.NomsStruct)

	return syncHeadRef.TargetHash(), syncInfo, nil
}

func (db *DB) MaybeEndSync(syncHead hash.Hash) ([]ReplayMutation, error) {
	syncHeadCommit, err := ReadCommit(db.Noms(), syncHead)
	if err != nil {
		return []ReplayMutation{}, err
	}

	defer db.lock()()
	head := db.head

	// Stop if someone landed a sync since this sync started (see explanation below).
	syncSnapshot, err := baseSnapshot(db.noms, syncHeadCommit)
	if err != nil {
		return []ReplayMutation{}, err
	}
	syncSnapshotBasis, err := syncSnapshot.Basis(db.noms)
	if err != nil {
		return []ReplayMutation{}, err
	}
	headSnapshot, err := baseSnapshot(db.noms, head)
	if err != nil {
		return []ReplayMutation{}, err
	}
	// BeginSync() added a new snapshot commit whose basis is the forkpoint.
	// E.g., in below diagram, BeginSync added SS2, the sync snapshot, and SS1
	// is the master snapshot basis and the forkpoint.
	// SS1 - L3 <- Master
	//   \ - SS2 - L1 - L2 <- SyncHead
	// However, the situation on master could have changed while this sync was running.
	// We need to check if the master snapshot basis is the same as SS1. If not,
	// some other sync landed a new snapshot on master and we have to abort. We do
	// not expect this in normal operation, we're being defensive.
	if !syncSnapshotBasis.NomsStruct.Equals(headSnapshot.NomsStruct) {
		return []ReplayMutation{}, fmt.Errorf("sync aborted: found a newer snapshot %s on master", headSnapshot.NomsStruct.Hash())
	}

	// Determine if there are any pending mutations that we need to replay.
	pendingCommits, err := pendingCommits(db.noms, head)
	if err != nil {
		return []ReplayMutation{}, err
	}
	commitsToReplay := filterIDsLessThanOrEqualTo(pendingCommits, syncHeadCommit.MutationID())
	var replay []ReplayMutation
	if len(commitsToReplay) > 0 {
		for _, c := range commitsToReplay {
			var args bytes.Buffer
			err = nomsjson.ToJSON(c.Meta.Local.Args, &args)
			if err != nil {
				return []ReplayMutation{}, err
			}
			replay = append(replay, ReplayMutation{
				Mutation{
					ID:   c.Meta.Local.MutationID,
					Name: string(c.Meta.Local.Name),
					Args: args.Bytes(),
				},
				&nomsjson.Hash{
					Hash: c.Ref().TargetHash(),
				},
			})
		}
		return replay, nil
	}

	// TODO check invariants from synchead back to syncsnapshot.

	// Sync is complete. Can't ffwd because sync head is dangling.
	_, err = db.noms.SetHead(db.noms.GetDataset(MASTER_DATASET), syncHeadCommit.Ref())
	if err != nil {
		return []ReplayMutation{}, err
	}
	db.head = syncHeadCommit

	return []ReplayMutation{}, nil
}

func filterIDsLessThanOrEqualTo(commits []Commit, filter uint64) (filtered []Commit) {
	for i := 0; i < len(commits); i++ {
		if commits[i].MutationID() > filter {
			filtered = append(filtered, commits[i])
		}
	}
	return
}
