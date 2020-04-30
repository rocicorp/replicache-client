package db

import (
	"fmt"
	"log"

	"github.com/attic-labs/noms/go/hash"
	servetypes "roci.dev/diff-server/serve/types"
)

type SyncInfo struct {
	BatchPushInfo  BatchPushInfo
	ClientViewInfo servetypes.ClientViewInfo
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
	syncHeadRef := db.noms.WriteValue(newSnapshot.Original)

	return syncHeadRef.TargetHash(), syncInfo, nil
}
