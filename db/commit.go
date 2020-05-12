package db

import (
	"errors"
	"fmt"

	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/marshal"
	"github.com/attic-labs/noms/go/nomdl"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/datetime"

	"roci.dev/diff-server/kv"
	"roci.dev/diff-server/util/chk"
	"roci.dev/diff-server/util/noms/union"
)

var (
	schema = nomdl.MustParseType(`
Struct Commit {
	parents: Set<Ref<Cycle<Commit>>>,
	// TODO: It would be cool to call this field "op" or something, but Noms requires a "meta"
	// top-level field.
	meta: Struct Snapshot {
		lastMutationID?: Number,
		serverStateID?: String,
	} |
	Struct Local {
		date:   Struct DateTime {
			secSinceEpoch: Number,
		},
		name: String,
		args: Value,
	},
	value: Struct {
		data: Ref<Map<String, Value>>,
		checksum: String,
	},
}`)
)

// TODO: These types should be private
type Local struct {
	MutationID uint64
	Date       datetime.DateTime
	Name       string
	Args       types.Value
	Original   types.Ref `noms:",omitempty"`
}

type Snapshot struct {
	LastMutationID uint64 `noms:",omitempty"`
	ServerStateID  string `noms:",omitempty"`
}

type Meta struct {
	// At most one of these will be set. If none are set, then the commit is the genesis commit.
	Local    Local    `noms:",omitempty"`
	Snapshot Snapshot `noms:",omitempty"`
}

func (m Meta) MarshalNoms(vrw types.ValueReadWriter) (val types.Value, err error) {
	v, err := union.Marshal(m, vrw)
	if err != nil {
		return nil, err
	}
	if v == nil {
		v = types.NewStruct("Snapshot", types.StructData{})
	}
	return v, nil
}

func (m *Meta) UnmarshalNoms(v types.Value) error {
	return union.Unmarshal(v, m)
}

type Commit struct {
	Parents []types.Ref `noms:",set"`
	Meta    Meta
	Value   struct {
		Data     types.Ref `noms:",omitempty"`
		Checksum types.String
	}
	NomsStruct types.Struct `noms:",original"`
}

type CommitType uint8

const (
	CommitTypeSnapshot = iota
	CommitTypeLocal
)

func (t CommitType) String() string {
	switch t {
	case CommitTypeLocal:
		return "CommitTypeLocal"
	case CommitTypeSnapshot:
		return "CommitTypeSnapshot"
	}
	chk.Fail("NOTREACHED")
	return ""
}

func (c Commit) NextMutationID() uint64 {
	return c.MutationID() + 1
}

func (c Commit) MutationID() uint64 {
	switch c.Type() {
	case CommitTypeLocal:
		return c.Meta.Local.MutationID
	case CommitTypeSnapshot:
		return c.Meta.Snapshot.LastMutationID
	}
	chk.Fail("NOTREACHED")
	return 0
}

func (c Commit) Ref() types.Ref {
	return types.NewRef(c.NomsStruct)
}

func (c Commit) Data(noms types.ValueReadWriter) kv.Map {
	return kv.FromNoms(noms, c.Value.Data.TargetValue(noms).(types.Map),
		kv.MustChecksumFromString(string(c.Value.Checksum)))
}

func (c Commit) Type() CommitType {
	if c.Meta.Local.Name != "" {
		return CommitTypeLocal
	}
	return CommitTypeSnapshot
}

func (c Commit) Original(noms types.ValueReadWriter) (Commit, error) {
	if c.Meta.Local.Original.IsZeroValue() {
		return Commit{}, nil
	}
	return ReadCommit(noms, c.Meta.Local.Original.TargetHash())
}

func (c Commit) BasisRef() types.Ref {
	switch len(c.Parents) {
	case 1:
		return c.Parents[0]
	}
	chk.Fail("Unexpected number of parents (%d) for commit with hash: %s", len(c.Parents), c.NomsStruct.Hash().String())
	return types.Ref{}
}

func (c Commit) BasisValue(noms types.ValueReader) types.Value {
	r := c.BasisRef()
	if r.IsZeroValue() {
		return nil
	}
	return r.TargetValue(noms)
}

func (c Commit) Basis(noms types.ValueReader) (Commit, error) {
	var r Commit
	err := marshal.Unmarshal(c.BasisValue(noms), &r)
	if err != nil {
		return Commit{}, err
	}
	return r, nil
}

func ReadCommit(noms types.ValueReadWriter, hash hash.Hash) (Commit, error) {
	if hash.IsEmpty() {
		return Commit{}, errors.New("commit (empty hash) not found")
	}
	v := noms.ReadValue(hash)
	if v == nil {
		return Commit{}, fmt.Errorf("commit %s not found", hash)
	}
	var c Commit
	err := marshal.Unmarshal(v, &c)
	return c, err
}

// Returns the commits in order (ie, earliest first and head last).
func pendingCommits(noms types.ValueReadWriter, head Commit) ([]Commit, error) {
	if head.Type() == CommitTypeSnapshot {
		return []Commit{}, nil
	}
	basis, err := head.Basis(noms)
	if err != nil {
		return []Commit{}, err
	}
	pending, err := pendingCommits(noms, basis)
	if err != nil {
		return []Commit{}, err
	}

	return append(pending, head), nil
}

func makeSnapshot(noms types.ValueReadWriter, basis types.Ref, serverStateID string, dataRef types.Ref, checksum types.String, lastMutationID uint64) Commit {
	c := Commit{}
	c.Parents = []types.Ref{basis}
	c.Meta.Snapshot.LastMutationID = lastMutationID
	c.Meta.Snapshot.ServerStateID = serverStateID
	c.Value.Data = dataRef
	c.Value.Checksum = checksum
	c.NomsStruct = marshal.MustMarshal(noms, c).(types.Struct)
	return c
}

// makeGenesis makes the first Snapshot, the Snapshot with no parents.
func makeGenesis(noms types.ValueReadWriter, serverStateID string, dataRef types.Ref, checksum types.String, lastMutationID uint64) Commit {
	c := Commit{}
	// Note: no c.Parents.
	c.Meta.Snapshot.LastMutationID = lastMutationID
	c.Meta.Snapshot.ServerStateID = serverStateID
	c.Value.Data = dataRef
	c.Value.Checksum = checksum
	c.NomsStruct = marshal.MustMarshal(noms, c).(types.Struct)
	return c
}

func makeLocal(noms types.ValueReadWriter, basis types.Ref, d datetime.DateTime, mutationID uint64, f string, args types.Value, newData types.Ref, checksum types.String) Commit {
	c := Commit{}
	c.Parents = []types.Ref{basis}
	c.Meta.Local.MutationID = mutationID
	c.Meta.Local.Date = d
	c.Meta.Local.Name = f
	c.Meta.Local.Args = args
	c.Value.Data = newData
	c.Value.Checksum = checksum
	c.NomsStruct = marshal.MustMarshal(noms, c).(types.Struct)
	return c
}

func makeReplayedLocal(noms types.ValueReadWriter, basis types.Ref, d datetime.DateTime, mutationID uint64, f string, args types.Value, newData types.Ref, checksum types.String, original types.Ref) Commit {
	c := Commit{}
	c.Parents = []types.Ref{basis}
	c.Meta.Local.MutationID = mutationID
	c.Meta.Local.Date = d
	c.Meta.Local.Name = f
	c.Meta.Local.Args = args
	c.Meta.Local.Original = original
	c.Value.Data = newData
	c.Value.Checksum = checksum
	c.NomsStruct = marshal.MustMarshal(noms, c).(types.Struct)
	return c
}
