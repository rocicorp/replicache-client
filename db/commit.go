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
	} |
	Struct Reorder {
		date:   Struct DateTime {
			secSinceEpoch: Number,
		},
		subject: Ref<Cycle<Commit>>,
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
	Original   hash.Hash
}

type Reorder struct {
	Date    datetime.DateTime
	Subject types.Ref
}

type Snapshot struct {
	LastMutationID uint64 `noms:",omitempty"`
	ServerStateID  string `noms:",omitempty"`
}

type Meta struct {
	// At most one of these will be set. If none are set, then the commit is the genesis commit.
	Local    Local    `noms:",omitempty"`
	Reorder  Reorder  `noms:",omitempty"`
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
	CommitTypeReorder
)

func (t CommitType) String() string {
	switch t {
	case CommitTypeLocal:
		return "CommitTypeLocal"
	case CommitTypeReorder:
		return "CommitTypeReorder"
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
	// TODO chk here once rebase commits are gone.
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
	if !c.Meta.Reorder.Subject.IsZeroValue() {
		return CommitTypeReorder
	}
	return CommitTypeSnapshot
}

// TODO: Rename to Subject to avoid confusion with ref.TargetValue().
func (c Commit) Target() types.Ref {
	if !c.Meta.Reorder.Subject.IsZeroValue() {
		return c.Meta.Reorder.Subject
	}
	return types.Ref{}
}

func (c Commit) Original(noms types.ValueReadWriter) (Commit, error) {
	if c.Meta.Local.Original.IsEmpty() {
		return Commit{}, nil
	}
	return ReadCommit(noms, c.Meta.Local.Original)
}

func (c Commit) InitalCommit(noms types.ValueReader) (Commit, error) {
	switch c.Type() {
	case CommitTypeLocal, CommitTypeSnapshot:
		return c, nil
	case CommitTypeReorder:
		var t Commit
		err := marshal.Unmarshal(c.Target().TargetValue(noms), &t)
		if err != nil {
			return Commit{}, err
		}
		return t.InitalCommit(noms)
	}
	return Commit{}, fmt.Errorf("Unexpected commit of type %v: %s", c.Type(), types.EncodedValue(c.NomsStruct))
}

func (c Commit) TargetValue(noms types.ValueReadWriter) types.Value {
	t := c.Target()
	if t.IsZeroValue() {
		return nil
	}
	return t.TargetValue(noms)
}

func (c Commit) TargetCommit(noms types.ValueReadWriter) (Commit, error) {
	tv := c.TargetValue(noms)
	if tv == nil {
		return Commit{}, nil
	}
	var r Commit
	err := marshal.Unmarshal(tv, &r)
	return r, err
}

func (c Commit) BasisRef() types.Ref {
	switch len(c.Parents) {
	case 0:
		return types.Ref{}
	case 1:
		return c.Parents[0]
	case 2:
		subj := c.Target()
		if subj.IsZeroValue() {
			chk.Fail("Unexpected 2-parent type of commit with hash: %s", c.NomsStruct.Hash().String())
		}
		for _, p := range c.Parents {
			if !p.Equals(subj) {
				return p
			}
		}
		chk.Fail("Unexpected state for commit with hash: %s", c.NomsStruct.Hash().String())
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

func makeReplayedLocal(noms types.ValueReadWriter, basis types.Ref, d datetime.DateTime, mutationID uint64, f string, args types.Value, newData types.Ref, checksum types.String, original Commit) Commit {
	c := Commit{}
	c.Parents = []types.Ref{basis}
	c.Meta.Local.MutationID = mutationID
	c.Meta.Local.Date = d
	c.Meta.Local.Name = f
	c.Meta.Local.Args = args
	c.Meta.Local.Original = original.NomsStruct.Hash()
	c.Value.Data = newData
	c.Value.Checksum = checksum
	c.NomsStruct = marshal.MustMarshal(noms, c).(types.Struct)
	return c
}

func makeReorder(noms types.ValueReadWriter, basis types.Ref, d datetime.DateTime, subject, newData types.Ref, checksum types.String) Commit {
	c := Commit{}
	c.Parents = []types.Ref{basis, subject}
	c.Meta.Reorder.Date = d
	c.Meta.Reorder.Subject = subject
	c.Value.Data = newData
	c.Value.Checksum = checksum
	c.NomsStruct = marshal.MustMarshal(noms, c).(types.Struct)
	return c
}
