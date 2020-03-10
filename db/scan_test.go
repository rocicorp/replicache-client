package db

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/stretchr/testify/assert"
)

func TestScan(t *testing.T) {
	assert := assert.New(t)
	sp, err := spec.ForDatabase("mem")
	assert.NoError(err)
	d, err := Load(sp)
	assert.NoError(err)

	put := func(k string) {
		err = d.Put(k, types.String(k))
		assert.NoError(err)
	}

	put("")
	put("a")
	put("ba")
	put("bb")

	index := func(v int) *uint64 {
		vv := uint64(v)
		return &vv
	}

	tc := []struct {
		opts          ScanOptions
		expected      []string
		expectedError error
	}{
		// no options
		{ScanOptions{}, []string{"", "a", "ba", "bb"}, nil},
		{ScanOptions{Start: &ScanBound{}}, []string{"", "a", "ba", "bb"}, nil},
		{ScanOptions{Start: &ScanBound{ID: &ScanID{}}}, []string{"", "a", "ba", "bb"}, nil},

		// prefix alone
		{ScanOptions{Prefix: "a"}, []string{"a"}, nil},
		{ScanOptions{Prefix: "b"}, []string{"ba", "bb"}, nil},
		{ScanOptions{Prefix: "b", Limit: 1}, []string{"ba"}, nil},
		{ScanOptions{Prefix: "b", Limit: 100}, []string{"ba", "bb"}, nil},
		{ScanOptions{Prefix: "c"}, []string{}, nil},

		// start.id alone
		{ScanOptions{Start: &ScanBound{ID: &ScanID{Value: "a"}}}, []string{"a", "ba", "bb"}, nil},
		{ScanOptions{Start: &ScanBound{ID: &ScanID{Value: "a", Exclusive: true}}}, []string{"ba", "bb"}, nil},
		{ScanOptions{Start: &ScanBound{ID: &ScanID{Value: "aa"}}}, []string{"ba", "bb"}, nil},
		{ScanOptions{Start: &ScanBound{ID: &ScanID{Value: "aa", Exclusive: true}}}, []string{"ba", "bb"}, nil},
		{ScanOptions{Start: &ScanBound{ID: &ScanID{Value: "a"}}, Limit: 2}, []string{"a", "ba"}, nil},
		{ScanOptions{Start: &ScanBound{ID: &ScanID{Value: "bb"}}}, []string{"bb"}, nil},
		{ScanOptions{Start: &ScanBound{ID: &ScanID{Value: "bb", Exclusive: true}}}, []string{}, nil},
		{ScanOptions{Start: &ScanBound{ID: &ScanID{Value: "c"}}}, []string{}, nil},

		// start.id and prefix together
		{ScanOptions{Prefix: "a", Start: &ScanBound{ID: &ScanID{Value: "a"}}}, []string{"a"}, nil},
		{ScanOptions{Prefix: "a", Start: &ScanBound{ID: &ScanID{Value: "b"}}}, []string{}, nil},
		{ScanOptions{Prefix: "b", Start: &ScanBound{ID: &ScanID{Value: "a"}}}, []string{"ba", "bb"}, nil},
		{ScanOptions{Prefix: "c", Start: &ScanBound{ID: &ScanID{Value: "a"}}}, []string{}, nil},
		{ScanOptions{Prefix: "a", Start: &ScanBound{ID: &ScanID{Value: "c"}}}, []string{}, nil},

		// start.index alone
		{ScanOptions{Start: &ScanBound{Index: index(0)}}, []string{"", "a", "ba", "bb"}, nil},
		{ScanOptions{Start: &ScanBound{Index: index(1)}}, []string{"a", "ba", "bb"}, nil},
		{ScanOptions{Start: &ScanBound{Index: index(1)}, Limit: 2}, []string{"a", "ba"}, nil},
		{ScanOptions{Start: &ScanBound{Index: index(4)}}, []string{}, nil},
		{ScanOptions{Start: &ScanBound{Index: index(100)}}, []string{}, nil},

		// start.index and start.id together
		{ScanOptions{Start: &ScanBound{Index: index(1), ID: &ScanID{Value: "b"}}}, []string{"ba", "bb"}, nil},
		{ScanOptions{Start: &ScanBound{Index: index(1), ID: &ScanID{Value: "b", Exclusive: true}}}, []string{"ba", "bb"}, nil},
		{ScanOptions{Start: &ScanBound{Index: index(1), ID: &ScanID{Value: "ba"}}}, []string{"ba", "bb"}, nil},
		{ScanOptions{Start: &ScanBound{Index: index(1), ID: &ScanID{Value: "ba", Exclusive: true}}}, []string{"bb"}, nil},
		{ScanOptions{Start: &ScanBound{Index: index(2), ID: &ScanID{Value: "a"}}}, []string{"ba", "bb"}, nil},
		{ScanOptions{Start: &ScanBound{Index: index(2), ID: &ScanID{Value: "a", Exclusive: true}}}, []string{"ba", "bb"}, nil},
		{ScanOptions{Start: &ScanBound{Index: index(4), ID: &ScanID{Value: "a"}}}, []string{}, nil},
		{ScanOptions{Start: &ScanBound{Index: index(1), ID: &ScanID{Value: "bb", Exclusive: true}}}, []string{}, nil},
		{ScanOptions{Start: &ScanBound{Index: index(1), ID: &ScanID{Value: "c"}}}, []string{}, nil},
		{ScanOptions{Start: &ScanBound{Index: index(1), ID: &ScanID{Value: "z"}}}, []string{}, nil},

		// prefix, start.index, and start.id together
		{ScanOptions{Prefix: "b", Start: &ScanBound{Index: index(1), ID: &ScanID{Value: "b"}}}, []string{"ba", "bb"}, nil},
		{ScanOptions{Prefix: "a", Start: &ScanBound{Index: index(1), ID: &ScanID{Value: "b"}}}, []string{}, nil},
		{ScanOptions{Prefix: "a", Start: &ScanBound{Index: index(0), ID: &ScanID{Value: "b"}}}, []string{}, nil},
		{ScanOptions{Prefix: "a", Start: &ScanBound{Index: index(0), ID: &ScanID{Value: "a"}}}, []string{"a"}, nil},
		{ScanOptions{Prefix: "c", Start: &ScanBound{Index: index(0), ID: &ScanID{Value: "a"}}}, []string{}, nil},
		{ScanOptions{Prefix: "a", Start: &ScanBound{Index: index(100), ID: &ScanID{Value: "a"}}}, []string{}, nil},
		{ScanOptions{Prefix: "a", Start: &ScanBound{Index: index(0), ID: &ScanID{Value: "z"}}}, []string{}, nil},
	}

	for i, t := range tc {
		js, err := json.Marshal(t.opts)
		assert.NoError(err)
		msg := fmt.Sprintf("case %d: %s", i, js)
		res, err := d.Scan(t.opts)
		if t.expectedError != nil {
			assert.Error(t.expectedError, err, msg)
			assert.Nil(res, msg)
			continue
		}
		assert.NoError(err)
		act := []string{}
		for _, it := range res {
			act = append(act, it.ID)
		}
		assert.Equal(t.expected, act, msg)
	}
}
