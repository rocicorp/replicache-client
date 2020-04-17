package db

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/attic-labs/noms/go/spec"
	"github.com/stretchr/testify/assert"
)

func TestScan(t *testing.T) {
	assert := assert.New(t)
	sp, err := spec.ForDatabase("mem")
	assert.NoError(err)
	d, err := Load(sp)
	assert.NoError(err)

	tx := d.NewTransaction()

	put := func(k string) {
		err = tx.Put(k, []byte(fmt.Sprintf("\"%s\"", k)))
		assert.NoError(err)
	}

	put("0")
	put("a")
	put("ba")
	put("bb")

	_, err = tx.Commit()
	assert.NoError(err)

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
		{ScanOptions{}, []string{"0", "a", "ba", "bb"}, nil},
		{ScanOptions{Start: &ScanBound{}}, []string{"0", "a", "ba", "bb"}, nil},
		{ScanOptions{Start: &ScanBound{ID: &ScanID{}}}, []string{"0", "a", "ba", "bb"}, nil},

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
		{ScanOptions{Start: &ScanBound{Index: index(0)}}, []string{"0", "a", "ba", "bb"}, nil},
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

	for i, testCase := range tc {
		js, err := json.Marshal(testCase.opts)
		assert.NoError(err)
		msg := fmt.Sprintf("case %d: %s", i, js)
		t.Run(string(js), func(t *testing.T) {
			tx := d.NewTransaction()
			defer tx.Close()

			res, err := tx.Scan(testCase.opts)
			if testCase.expectedError != nil {
				assert.Error(testCase.expectedError, err, msg)
				assert.Nil(res, msg)
				return
			}
			assert.NoError(err)
			act := []string{}
			for _, it := range res {
				act = append(act, it.Key)
			}
			assert.Equal(testCase.expected, act, msg)
		})
	}
}
