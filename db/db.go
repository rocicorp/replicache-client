// Package db implements the core database abstraction of Replicache.
package db

import (
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/marshal"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"

	"roci.dev/diff-server/kv"
	jsnoms "roci.dev/diff-server/util/noms/json"
)

const (
	MASTER_DATASET = "master"
)

type DB struct {
	noms     datas.Database
	clientID string
	pusher   pusher
	puller   puller

	mu   sync.Mutex
	head Commit
}

func Load(sp spec.Spec) (*DB, error) {
	if !sp.Path.IsEmpty() {
		return nil, errors.New("Invalid spec - must not specify a path")
	}

	var noms datas.Database
	err := d.Try(func() {
		noms = sp.GetDatabase()
	})
	if err != nil {
		err = err.(d.WrappedError).Cause()
		return nil, err
	}
	return New(noms)
}

func New(noms datas.Database) (*DB, error) {
	r := DB{
		noms:   noms,
		pusher: defaultPusher{},
		puller: defaultPuller{},
	}
	// Of course nothing could have a handle on r yet, but still good practice.
	defer r.lock()()
	err := r.initLocked()
	if err != nil {
		return nil, err
	}

	return &r, nil
}

// initLocked initializes the DB for use. The mutex must be held when called.
func (db *DB) initLocked() error {
	var err error

	cid := db.clientID
	if cid == "" {
		cid, err = initClientID(db.noms)
		// TODO create obfuscated clientID for data layer here as well.
		log.Printf("ClientID: %s", cid)
	}
	if err != nil {
		return err
	}
	db.clientID = cid

	ds := db.noms.GetDataset(MASTER_DATASET)
	if !ds.HasHead() {
		m := kv.NewMap(db.noms)
		genesis := makeGenesis(db.noms, "", db.noms.WriteValue(m.NomsMap()), m.NomsChecksum(), 0 /*lastMutationID*/)
		genRef := db.noms.WriteValue(genesis.NomsStruct)
		_, err := db.noms.FastForward(ds, genRef)
		if err != nil {
			return err
		}
		db.head = genesis
		return nil
	}

	headType := types.TypeOf(ds.Head())
	if !types.IsSubtype(schema, headType) {
		return fmt.Errorf("Cannot load database. Specified head has non-Replicache data of type: %s", headType.Describe())
	}

	var head Commit
	err = marshal.Unmarshal(ds.Head(), &head)
	if err != nil {
		return err
	}
	db.head = head
	return nil
}

func (db *DB) Noms() types.ValueReadWriter {
	return db.noms
}

func (db *DB) Head() Commit {
	defer db.lock()()
	return db.head
}

// setHead sets the head commit to newHead and fast-forwards the underlying dataset.
func (db *DB) setHead(newHead Commit) error {
	defer db.lock()()
	_, err := db.noms.FastForward(db.noms.GetDataset(MASTER_DATASET), newHead.Ref())
	if err != nil {
		return err
	}
	db.head = newHead
	return nil
}

func (db *DB) HeadHash() hash.Hash {
	return db.Head().NomsStruct.Hash()
}

func (db *DB) Reload() error {
	defer db.lock()()
	db.noms.Rebase()
	return db.initLocked()
}

// TODO: add date and random source to this so that sync can set it up correctly when replaying.
func (db *DB) execImpl(basis types.Ref, function string, args types.Value) (newDataRef types.Ref, newDataChecksum types.String, output types.Value, isWrite bool, err error) {
	var basisCommit Commit
	err = marshal.Unmarshal(basis.TargetValue(db.noms), &basisCommit)
	if err != nil {
		return types.Ref{}, types.String(""), nil, false, err
	}

	newData := basisCommit.Value.Data

	if strings.HasPrefix(function, ".") {
		switch function {
		case ".putValue":
			if _, ok := args.(types.List); !ok {
				err = fmt.Errorf("Internal error. Expected a List but got %s", types.TypeOf(args).Describe())
				return
			}
			k := args.(types.List).Get(0).(types.String)
			v := args.(types.List).Get(1)
			ed := basisCommit.Data(db.noms).Edit()
			isWrite = true
			err = ed.Set(k, v)
			if err != nil {
				err = fmt.Errorf("could not Put '%s'='%s': %w", k, v, err)
				return
			}
			newMap := ed.Build()
			newDataChecksum = newMap.NomsChecksum()
			newData = db.noms.WriteValue(newMap.NomsMap())
			break

		case ".delValue":
			if _, ok := args.(types.List); !ok {
				err = fmt.Errorf("Internal error. Expected a List but got %s", types.TypeOf(args).Describe())
				return
			}
			k := args.(types.List).Get(0).(types.String)
			m := basisCommit.Data(db.noms)
			ed := m.Edit()
			isWrite = true
			ok := ed.Has(k)
			err = ed.Remove(k)
			if err != nil {
				err = fmt.Errorf("could not Del '%s': %w", k, err)
				return
			}
			newMap := ed.Build()
			newData = db.noms.WriteValue(newMap.NomsMap())
			newDataChecksum = newMap.NomsChecksum()
			output = types.Bool(ok)
			break
		}
	} else {
		d.Panic("NON-INTERNAL TRANSACTIONS DISABLED FOR NOW")
	}

	return newData, newDataChecksum, output, isWrite, nil
}

// NewTransaction returns a new Transaction.
func (db *DB) NewTransaction() *Transaction {
	return db.NewTransactionWithArgs("", jsnoms.Null(), nil, nil)
}

// NewTransactionWithArgs creates a new transaction with a name and arguments.
// The name and the arguments are used when replaying transactions. Basis and
// original should be non-nil for replay transactions.
func (db *DB) NewTransactionWithArgs(name string, args types.Value, basis *Commit, original *Commit) *Transaction {
	head := db.Head()
	if basis != nil {
		head = *basis
	}

	return &Transaction{
		db:       db,
		basis:    head,
		me:       head.Data(db.noms).Edit(),
		name:     name,
		args:     args,
		original: original,
	}
}

func (db *DB) lock() func() {
	db.mu.Lock()
	return func() {
		db.mu.Unlock()
	}
}
