package db

import (
	"fmt"

	"github.com/attic-labs/noms/go/marshal"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/datetime"
	"github.com/stretchr/testify/assert"
	"roci.dev/diff-server/kv"
)

type testCommits []Commit

func (t testCommits) genesis() Commit {
	return t[0]
}

func (t testCommits) head() Commit {
	return t[len(t)-1]
}

func (t *testCommits) addGenesis(assert *assert.Assertions, db *DB) *testCommits {
	m := kv.NewMap(db.noms)
	genesis := makeGenesis(db.noms, "", db.noms.WriteValue(m.NomsMap()), m.NomsChecksum(), 0)
	db.noms.WriteValue(marshal.MustMarshal(db.noms, genesis.Original))
	*t = append(*t, genesis)
	return t
}

func (t *testCommits) addSnapshot(assert *assert.Assertions, db *DB) *testCommits {
	m := kv.NewMap(db.noms)
	basis := (*t).head()
	snapshot := makeSnapshot(db.noms, basis.Ref(), fmt.Sprintf("ssid%d", len(*t)-1), db.Noms().WriteValue(m.NomsMap()), m.NomsChecksum(), basis.MutationID())
	db.noms.WriteValue(marshal.MustMarshal(db.noms, snapshot.Original))
	*t = append(*t, snapshot)
	return t
}

func (t *testCommits) addLocal(assert *assert.Assertions, db *DB, d datetime.DateTime) *testCommits {
	m := kv.NewMap(db.noms)
	basis := (*t).head()
	local := makeLocal(db.noms, basis.Ref(), d, basis.NextMutationID(), fmt.Sprintf("TxName%d", len(*t)-1), types.NewList(db.noms), db.Noms().WriteValue(m.NomsMap()), m.NomsChecksum())
	db.noms.WriteValue(marshal.MustMarshal(db.noms, local.Original))
	*t = append(*t, local)
	return t
}