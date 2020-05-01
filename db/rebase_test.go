package db

import (
	"fmt"
	"testing"

	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/datetime"
	"github.com/stretchr/testify/assert"

	"roci.dev/diff-server/kv"
	"roci.dev/diff-server/util/noms/diff"
)

func TestRebase(t *testing.T) {
	assert := assert.New(t)

	db, dir := LoadTempDB(assert)
	fmt.Println(dir)
	noms := db.noms

	list := func(items ...string) types.List {
		r := types.NewList(noms).Edit()
		for i := 0; i < len(items); i++ {
			r.Append(types.String(items[i]))
		}
		return r.List()
	}

	write := func(v types.Value) types.Ref {
		return noms.WriteValue(v)
	}

	data := func(ds string) kv.Map {
		if ds == "" {
			return kv.NewMap(noms)
		}
		nm := types.NewMap(noms, types.String("foo"), types.String(ds))
		return kv.FromNoms(noms, nm, kv.ComputeChecksum(nm))
	}

	assertEqual := func(c1, c2 Commit) {
		if c1.NomsStruct.Equals(c2.NomsStruct) {
			return
		}
		fmt.Println(c1.Data(noms).DebugString(), c2.Data(noms).DebugString())
		fmt.Println(c1.NomsStruct.Hash(), c2.NomsStruct.Hash())
		assert.Fail("Commits are unequal", "expected: %s, actual: %s, diff: %s", c1.NomsStruct.Hash(), c2.NomsStruct.Hash(), diff.Diff(c1.NomsStruct, c2.NomsStruct))
	}

	g := db.head
	epoch := datetime.DateTime{}

	tx := func(basis Commit, arg string, ds string) Commit {
		m := data(ds)
		r := makeLocal(
			noms,
			basis.Ref(),
			epoch,
			basis.NextMutationID(),
			".putValue",                          // function
			list("foo", arg),                     // args
			write(m.NomsMap()), m.NomsChecksum()) // result data
		write(r.NomsStruct)
		return r
	}

	ro := func(basis, subject Commit, ds string) Commit {
		m := data(ds)
		r := makeReorder(
			noms,
			basis.Ref(),
			epoch,
			subject.Ref(),
			write(m.NomsMap()), m.NomsChecksum()) // result data
		write(r.NomsStruct)
		return r
	}

	test := func(onto, head, expected Commit, expectedError string) {
		noms.Flush()
		actual, err := rebase(db, onto.Ref(), epoch, head, types.Ref{})
		if expectedError != "" {
			assert.EqualError(err, expectedError)
			return
		}
		assert.NoError(err)
		write(actual.NomsStruct)
		noms.Flush()
		assertEqual(expected, actual)
	}
	// dest ff
	// onto: g
	// head: g - a
	// rslt: g - a
	(func() {
		a := tx(g, "a", "a")
		test(g, a, a, "")
	})()

	// https://github.com/aboodman/replicant/issues/68
	// same as dest ff, except where there's also a 'local' branch whose head is > onto
	// local: g - a
	// onto:  g
	// head:  g - a
	// rslt:  g - a
	(func() {
		a := tx(g, "a", "a")
		_, err := noms.SetHead(noms.GetDataset(LOCAL_DATASET), a.Ref())
		assert.NoError(err)
		db.Reload()
		test(g, a, a, "")
	})()

	// source ff
	// onto: g - a
	// head: g
	// rslt: g - a
	(func() {
		a := tx(g, "a", "a")
		test(a, g, a, "")
	})()

	// simple reorder
	// onto: g - a
	// head: g - b
	// rslt: g - a - ro(b)
	//         \ b /
	(func() {
		a := tx(g, "a", "a")
		b := tx(g, "b", "b")
		expected := ro(a, b, "b")
		test(a, b, expected, "")
	})()

	// chained reorder
	// onto: g - a
	// head: g - b - c
	// rslt: g - a - ro(b) - ro(c)
	//         \ b /         /
	//            \ ------- c
	(func() {
		a := tx(g, "a", "a")
		b := tx(g, "b", "b")
		c := tx(b, "c", "c")
		rob := ro(a, b, "b")
		roc := ro(rob, c, "c")
		test(a, c, roc, "")
	})()

	// re-reorder
	// onto: g - a - b
	// head: g - a - ro(c)
	//         \ c /
	// rslt: g - a -  b  -  ro(ro(c))
	//         \    \ ro(c) /
	//          \  c  /
	(func() {
		a := tx(g, "a", "a")
		b := tx(a, "b", "b")
		c := tx(g, "c", "c")
		roc := ro(a, c, "c")
		expected := ro(b, roc, "c")
		test(b, roc, expected, "")
	})()
}
