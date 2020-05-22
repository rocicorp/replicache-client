package db

import (
	"bytes"
	"fmt"

	"github.com/attic-labs/noms/go/hash"
	zl "github.com/rs/zerolog"
	servetypes "roci.dev/diff-server/serve/types"
	nomsjson "roci.dev/diff-server/util/noms/json"
)

type SyncInfo struct {
	// SyncID uniquely identifies this sync for the purposes of logging and debugging.
	SyncID string `json:"syncID"`
	// BatchPushInfo will be set if we attempted to push, ie if there were >0 pending commits.
	// Status code will be 0 if the request was not sent (eg, connection refused). The
	// ErrorMessage will be filled in if an error occurred, eg with the http response body
	// if we got a non-200 response code or "connection refused" if we couldn't make the
	// request. Note it is possible to have a 200 status code *and* an ErrorMessage, eg
	// the response code was 200 but we couldn't parse the response body.
	BatchPushInfo *BatchPushInfo `json:"batchPushInfo,omitempty"`
	// ClientViewInfo will be set if the request to the diffserver completed with status 200
	// and the diffserver attempted to request the client view from the data layer.
	ClientViewInfo servetypes.ClientViewInfo `json:"clientViewInfo"`
}

// BeginSync initiates the sync process, temporarily forking the cache
// onto a new branch called the "sync head", pushing pending mutations
// to the data layer, and pulling any new state from the data layer via
// diff-server.
//
// If a non-zero syncHead is returned, then caller should call
// MaybeEndSync (potentially multiple times in a loop) to finalize
// the sync. See MaybeEndSync for details.
//
// Informational details about the push and pull requests are returned
// via SyncInfo.
//
// Returns an error (and zeros for other return values) in the case of
// invalid argument values, or internal errors.
func (db *DB) BeginSync(batchPushURL string, diffServerURL string, diffServerAuth string, dataLayerAuth string, l zl.Logger) (syncHead hash.Hash, syncInfo SyncInfo, err error) {
	syncInfo = SyncInfo{}
	syncInfo.SyncID = db.newSyncID()
	l = l.With().Str("syncID", syncInfo.SyncID).Logger()
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
		pushInfo := db.pusher.Push(mutations, batchPushURL, dataLayerAuth, db.clientID, syncInfo.SyncID)
		syncInfo.BatchPushInfo = &pushInfo
		l.Debug().Msgf("Batch push finished with status %d error message '%s'", syncInfo.BatchPushInfo.HTTPStatusCode, syncInfo.BatchPushInfo.ErrorMessage)
		// Note: we always continue whether the push succeeded or not.
	}

	// Pull
	headSnapshot, err := baseSnapshot(db.noms, head)
	if err != nil {
		return hash.Hash{}, syncInfo, fmt.Errorf("could not find head snapshot: %w", err)
	}
	newSnapshot, clientViewInfo, err := db.puller.Pull(db.noms, headSnapshot, diffServerURL, diffServerAuth, dataLayerAuth, db.clientID, syncInfo.SyncID)
	if err != nil {
		return hash.Hash{}, syncInfo, fmt.Errorf("pull from %s failed: %w", diffServerURL, err)
	}
	syncInfo.ClientViewInfo = clientViewInfo
	if newSnapshot.Meta.Snapshot.ServerStateID == headSnapshot.Meta.Snapshot.ServerStateID {
		return hash.Hash{}, syncInfo, nil
	}
	syncHeadRef := db.noms.WriteValue(newSnapshot.NomsStruct)

	return syncHeadRef.TargetHash(), syncInfo, nil
}

// MaybeEndSync attempts to finalize a sync initiated by BeginSync() by
// switching master to point to the syncHead. However, if there are
// pending commits that have not yet been included in latest snapshot,
// then finalization is not yet possible. In that case, those commits
// that must be replayed are returned. Caller must replay them, then
// call MaybeEndSync again.
func (db *DB) MaybeEndSync(syncHead hash.Hash, syncID string) ([]ReplayMutation, error) {
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
	// SS1 - L1 <- Master
	//   \ - SS2 <- SyncHead
	// However, the situation on master could have changed while this sync was running.
	// Another sync might have landed a different sync snapshot, SS3:
	// SS1 - SS3 - L1 <- Master
	//   \ - SS2 <- SyncHead
	// We need to check if the master snapshot basis is the same as SS1. If not,
	// some other sync landed a new snapshot on master and we have to abort. We do
	// not expect this in normal operation, we're being defensive.
	if !syncSnapshotBasis.NomsStruct.Equals(headSnapshot.NomsStruct) {
		return []ReplayMutation{}, fmt.Errorf("found a newer snapshot %s on master", headSnapshot.NomsStruct.Hash())
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
