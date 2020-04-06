package db

import (
	"strings"

	"github.com/attic-labs/noms/go/types"

	"roci.dev/diff-server/util/chk"
	jsnoms "roci.dev/diff-server/util/noms/json"
)

const (
	defaultScanLimit = 50
)

type ScanID struct {
	Value     string `json:"value,omitempty"`
	Exclusive bool   `json:"exclusive,omitempty"`
}

type ScanBound struct {
	ID    *ScanID `json:"id,omitempty"`
	Index *uint64 `json:"index,omitempty"`
}

type ScanOptions struct {
	Prefix string     `json:"prefix,omitempty"`
	Start  *ScanBound `json:"start,omitempty"`
	Limit  int        `json:"limit,omitempty"`
	// Future: EndAtID, EndBeforeID
}

type ScanItem struct {
	ID    string       `json:"id"`
	Value jsnoms.Value `json:"value"`
}

func (db *DB) Scan(opts ScanOptions) ([]ScanItem, error) {
	// TODO fritz clean up
	return scan(db.head.Data(db.noms).NomsMap(), opts)
}

func scan(data types.Map, opts ScanOptions) ([]ScanItem, error) {
	var it *types.MapIterator

	updateIter := func(cand *types.MapIterator) {
		if it == nil {
			it = cand
		} else if !it.Valid() {
			// the current iterator is at the end, no value could be greater
		} else if !cand.Valid() {
			// the candidate is at the end, all values are less
			it = cand
		} else if it.Key().Less(cand.Key()) {
			it = cand
		} else {
			// the current iterator is >= the candidate
		}
	}

	if opts.Prefix != "" {
		updateIter(data.IteratorFrom(types.String(opts.Prefix)))
	}

	if opts.Start != nil {
		if opts.Start.ID != nil && opts.Start.ID.Value != "" {
			sk := types.String(opts.Start.ID.Value)
			it := data.IteratorFrom(sk)
			if opts.Start.ID.Exclusive && it.Valid() && it.Key().Equals(sk) {
				it.Next()
			}
			updateIter(it)
		}
		if opts.Start.Index != nil {
			updateIter(data.IteratorAt(uint64((*opts.Start.Index))))
		}
	}

	if it == nil {
		it = data.Iterator()
	}

	lim := opts.Limit
	if lim == 0 {
		lim = 50
	}

	res := []ScanItem{}
	for ; it.Valid(); it.Next() {
		k, v := it.Entry()
		chk.True(k.Kind() == types.StringKind, "Only keys with string kinds are supported, Noms schema check should have caught this")
		ks := string(k.(types.String))
		if opts.Prefix != "" && !strings.HasPrefix(ks, opts.Prefix) {
			break
		}
		res = append(res, ScanItem{
			ID:    ks,
			Value: jsnoms.Make(nil, v),
		})
		if len(res) == lim {
			break
		}
	}
	return res, nil
}
