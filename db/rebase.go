package db

import (
	"fmt"

	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/marshal"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/datetime"
)

// rebase transforms a forked commit history into a linear one by moving one side
// of the fork such that it comes after the other side.
// Specifically rebase finds the forkpoint between `commit` and `onto`. The commits
// after this forkpoint on the `commit` side are replayed one by one on top of onto,
// and the resulting new head is returned.
//
// In Replicache, unlike e.g., Git, this is done such that the original forked
// history is still preserved in the database (e.g. for later debugging). But the
// effect on the data and from user's point of view is the same as `git rebase`.
func rebase(db *DB, onto types.Ref, date datetime.DateTime, commit Commit, forkPoint types.Ref) (rebased Commit, err error) {
	if forkPoint.IsZeroValue() {
		forkPoint, err = commonAncestor(onto, commit.Ref(), db.Noms())
		if err != nil {
			return rebased, err
		}
	}

	// If we've reached out forkpoint then by definition `onto` is the result.
	if commit.Ref().Equals(forkPoint) {
		var r Commit
		err = marshal.Unmarshal(onto.TargetValue(db.noms), &r)
		if err != nil {
			return Commit{}, err
		}
		return r, nil
	}

	// Otherwise, we recurse on this commit's basis.
	oldBasis, err := commit.Basis(db.noms)
	if err != nil {
		return Commit{}, err
	}
	newBasis, err := rebase(db, onto, date, oldBasis, forkPoint)
	if err != nil {
		return Commit{}, err
	}

	// If the current and desired basis match, this is a fast-forward, and there's nothing to do.
	if newBasis.NomsStruct.Equals(oldBasis.NomsStruct) {
		return commit, nil
	}

	// Otherwise we need to re-execute the transaction against the new basis.
	var newData types.Ref
	var newDataChecksum types.String

	switch commit.Type() {
	case CommitTypeLocal:
		// For Local transactions, just re-run the tx with the new basis.
		newData, newDataChecksum, _, _, err = db.execImpl(newBasis.Ref(), commit.Meta.Local.Name, commit.Meta.Local.Args)
		if err != nil {
			return Commit{}, err
		}
		break

	case CommitTypeReorder:
		// Reorder transactions can be recursive. But at the end of the chain there will eventually be an original Local function.
		// Find it and re-run it against the new basis.
		target, err := commit.InitalCommit(db.noms)
		if err != nil {
			return Commit{}, err
		}
		newData, newDataChecksum, _, _, err = db.execImpl(newBasis.Ref(), target.Meta.Local.Name, target.Meta.Local.Args)
		if err != nil {
			return Commit{}, err
		}

	}

	// Create and return the reorder commit, which will become the basis for the prev frame of the recursive call.
	newCommit := makeReorder(db.noms, newBasis.Ref(), date, commit.Ref(), newData, newDataChecksum)
	db.noms.WriteValue(newCommit.NomsStruct)
	return newCommit, nil
}

func commonAncestor(r1, r2 types.Ref, noms types.ValueReader) (a types.Ref, err error) {
	fp, ok := datas.FindCommonAncestor(r1, r2, noms)
	if !ok {
		return a, fmt.Errorf("No common ancestor between commits: %s and %s", r1.TargetHash(), r2.TargetHash())
	}
	return fp, nil
}
