package db

import (
	"fmt"
	"testing"

	"github.com/attic-labs/noms/go/chunks"
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
	emkvm := kv.NewMap(noms)
	emptyMap := noms.WriteValue(emkvm.NomsMap())
	emptyMapChecksum := types.String(emkvm.Checksum().String())

	d := datetime.Now()
	drkvm := kv.NewMapFromNoms(noms, types.NewMap(noms, types.String("foo"), types.String("bar")))
	drChecksum := types.String(drkvm.Checksum().String())
	dr := noms.WriteValue(drkvm.NomsMap())
	args := types.NewList(noms, types.Bool(true), types.String("monkey"))
	g := makeGenesis(noms, "", emptyMap, emptyMapChecksum)
	tx := makeTx(noms, types.NewRef(g.Original), d, "func", args, dr, drChecksum)
	noms.WriteValue(g.Original)
	noms.WriteValue(tx.Original)

	tc := []struct {
		in  Commit
		exp types.Value
	}{
		{
			makeGenesis(noms, "", emptyMap, emptyMapChecksum),
			types.NewStruct("Commit", types.StructData{
				"meta":    types.NewStruct("Genesis", types.StructData{}),
				"parents": types.NewSet(noms),
				"value": types.NewStruct("", types.StructData{
					"data":     emptyMap,
					"checksum": emptyMapChecksum,
				}),
			}),
		},
		{
			makeGenesis(noms, "foo", emptyMap, emptyMapChecksum),
			types.NewStruct("Commit", types.StructData{
				"meta": types.NewStruct("Genesis", types.StructData{
					"serverStateID": types.String("foo"),
				}),
				"parents": types.NewSet(noms),
				"value": types.NewStruct("", types.StructData{
					"data":     emptyMap,
					"checksum": emptyMapChecksum,
				}),
			}),
		},
		{
			tx,
			types.NewStruct("Commit", types.StructData{
				"parents": types.NewSet(noms, types.NewRef(g.Original)),
				"meta": types.NewStruct("Tx", types.StructData{
					"date": marshal.MustMarshal(noms, d),
					"name": types.String("func"),
					"args": args,
				}),
				"value": types.NewStruct("", types.StructData{
					"data":     dr,
					"checksum": drChecksum,
				}),
			}),
		},
		{
			makeTx(noms, types.NewRef(g.Original), d, "func", args, dr, drChecksum),
			types.NewStruct("Commit", types.StructData{
				"parents": types.NewSet(noms, types.NewRef(g.Original)),
				"meta": types.NewStruct("Tx", types.StructData{
					"date": marshal.MustMarshal(noms, d),
					"name": types.String("func"),
					"args": args,
				}),
				"value": types.NewStruct("", types.StructData{
					"data":     dr,
					"checksum": drChecksum,
				}),
			}),
		},
		{
			makeReorder(noms, types.NewRef(g.Original), d, types.NewRef(tx.Original), dr, drChecksum),
			types.NewStruct("Commit", types.StructData{
				"parents": types.NewSet(noms, types.NewRef(g.Original), types.NewRef(tx.Original)),
				"meta": types.NewStruct("Reorder", types.StructData{
					"date":    marshal.MustMarshal(noms, d),
					"subject": types.NewRef(tx.Original),
				}),
				"value": types.NewStruct("", types.StructData{
					"data":     dr,
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
		assert.NoError(err)

		remarshalled, err := marshal.Marshal(noms, roundtrip)
		assert.NoError(err)
		assert.True(act.Equals(remarshalled), fmt.Sprintf("test case %d", i))
	}
}
