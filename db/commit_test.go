package db

import (
	"fmt"
	"testing"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/marshal"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/datetime"
	"github.com/stretchr/testify/assert"

	"roci.dev/diff-server/kv"
	"roci.dev/diff-server/util/noms/diff"
)

func TestMarshal(t *testing.T) {
	assert := assert.New(t)

	noms := types.NewValueStore((&chunks.TestStorage{}).NewView())
	em := kv.NewMap(noms)
	emRef := noms.WriteValue(em.NomsMap())
	emChecksum := em.NomsChecksum()
	emLTID := uint64(1)

	d := datetime.Now()
	dr := kv.NewMapForTest(noms, "foo", `"bar"`)
	drChecksum := dr.NomsChecksum()
	drRef := noms.WriteValue(dr.NomsMap())
	args := types.NewList(noms, types.Bool(true), types.String("monkey"))
	g := makeGenesis(noms, "", emRef, emChecksum, emLTID)
	tx := makeLocal(noms, g.Ref(), d, g.NextMutationID(), "func", args, drRef, drChecksum)
	noms.WriteValue(g.NomsStruct)

	tc := []struct {
		in  Commit
		exp types.Value
	}{
		{
			makeGenesis(noms, "", emRef, emChecksum, uint64(0)),
			types.NewStruct("Commit", types.StructData{
				"meta":    types.NewStruct("Snapshot", types.StructData{}),
				"parents": types.NewSet(noms),
				"value": types.NewStruct("", types.StructData{
					"data":     emRef,
					"checksum": emChecksum,
				}),
			}),
		},
		{
			makeSnapshot(noms, g.Ref(), "foo", emRef, emChecksum, emLTID),
			types.NewStruct("Commit", types.StructData{
				"meta": types.NewStruct("Snapshot", types.StructData{
					"lastMutationID": types.Number(emLTID),
					"serverStateID":  types.String("foo"),
				}),
				"parents": types.NewSet(noms, g.Ref()),
				"value": types.NewStruct("", types.StructData{
					"data":     emRef,
					"checksum": emChecksum,
				}),
			}),
		},
		{
			tx,
			types.NewStruct("Commit", types.StructData{
				"parents": types.NewSet(noms, g.Ref()),
				"meta": types.NewStruct("Local", types.StructData{
					"mutationID": types.Number(g.NextMutationID()),
					"date":       marshal.MustMarshal(noms, d),
					"name":       types.String("func"),
					"args":       args,
					"original":   marshal.MustMarshal(noms, hash.Hash{}),
				}),
				"value": types.NewStruct("", types.StructData{
					"data":     drRef,
					"checksum": drChecksum,
				}),
			}),
		},
		{
			makeLocal(noms, g.Ref(), d, g.NextMutationID(), "func", args, drRef, drChecksum),
			types.NewStruct("Commit", types.StructData{
				"parents": types.NewSet(noms, g.Ref()),
				"meta": types.NewStruct("Local", types.StructData{
					"mutationID": types.Number(g.NextMutationID()),
					"date":       marshal.MustMarshal(noms, d),
					"name":       types.String("func"),
					"args":       args,
					"original":   marshal.MustMarshal(noms, hash.Hash{}),
				}),
				"value": types.NewStruct("", types.StructData{
					"data":     drRef,
					"checksum": drChecksum,
				}),
			}),
		},
		{
			makeReplayedLocal(noms, g.Ref(), d, g.NextMutationID(), "func", args, drRef, drChecksum, tx),
			types.NewStruct("Commit", types.StructData{
				"parents": types.NewSet(noms, g.Ref()),
				"meta": types.NewStruct("Local", types.StructData{
					"mutationID": types.Number(g.NextMutationID()),
					"date":       marshal.MustMarshal(noms, d),
					"name":       types.String("func"),
					"args":       args,
					"original":   marshal.MustMarshal(noms, tx.Ref().TargetHash()),
				}),
				"value": types.NewStruct("", types.StructData{
					"data":     drRef,
					"checksum": drChecksum,
				}),
			}),
		},
		{
			makeReorder(noms, g.Ref(), d, tx.Ref(), drRef, drChecksum),
			types.NewStruct("Commit", types.StructData{
				"parents": types.NewSet(noms, g.Ref(), tx.Ref()),
				"meta": types.NewStruct("Reorder", types.StructData{
					"date":    marshal.MustMarshal(noms, d),
					"subject": tx.Ref(),
				}),
				"value": types.NewStruct("", types.StructData{
					"data":     drRef,
					"checksum": drChecksum,
				}),
			}),
		},
	}

	for i, t := range tc {
		act, err := marshal.Marshal(noms, t.in)
		assert.NoError(err, "test case: %d", i)
		assert.True(t.exp.Equals(act), "test case: %d - %s", i, diff.Diff(t.exp, act))

		var roundtrip Commit
		err = marshal.Unmarshal(act, &roundtrip)
		assert.NoError(err, "test case: %d", i)

		remarshalled, err := marshal.Marshal(noms, roundtrip)
		assert.NoError(err, "test case: %d", i)
		assert.True(act.Equals(remarshalled), fmt.Sprintf("test case %d", i))
	}
}
