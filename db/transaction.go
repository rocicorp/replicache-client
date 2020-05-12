package db

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"sync"

	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/types"

	"roci.dev/diff-server/kv"
	nomsjson "roci.dev/diff-server/util/noms/json"
	"roci.dev/diff-server/util/time"
)

var (
	// ErrClosed is the error returned from operations on a Transaction when
	// it has already been closed.
	ErrClosed = errors.New("Transaction is closed")
)

// Transaction represents a read and write transaction. Changes to the database
// are not committed until Commit is called.
// Transactions are thread safe.
type Transaction struct {
	db       *DB
	basis    Commit
	me       *kv.MapEditor
	wrote    bool
	closed   bool
	name     string
	args     types.Value
	original *Commit // non-nil for replay transactions.

	mutex sync.RWMutex
}

func (tx *Transaction) rlock() func() {
	tx.mutex.RLock()
	return func() {
		tx.mutex.RUnlock()
	}
}

func (tx *Transaction) lock() func() {
	tx.mutex.Lock()
	return func() {
		tx.mutex.Unlock()
	}
}

// IsReplay returns true if the transaction is a replay.
func (tx Transaction) IsReplay() bool {
	return tx.original != nil
}

// Closed returns true when the transaction has been closed. A transaction
// becomes closed after Commit or Close is called.
func (tx *Transaction) Closed() bool {
	defer tx.rlock()()
	return tx.closed
}

// Get returns the JSON encoded value at the given id of the database.
func (tx *Transaction) Get(id string) ([]byte, error) {
	defer tx.rlock()()

	if tx.closed {
		return nil, ErrClosed
	}
	value := tx.me.Get(types.String(id))
	if value == nil {
		return nil, nil
	}
	var b bytes.Buffer
	err := nomsjson.ToJSON(value, &b)
	return b.Bytes(), err
}

// Has returns true if the database has an entry with the given id.
func (tx *Transaction) Has(id string) (bool, error) {
	defer tx.rlock()()

	if tx.closed {
		return false, ErrClosed
	}
	return tx.me.Has(types.String(id)), nil
}

// Scan returns a slice of ScanItems of the id-value pairs in the database. You
// can use ScanOptions to get all the items with a certain prefix or limit the
// number of results.
func (tx *Transaction) Scan(opts ScanOptions) ([]ScanItem, error) {
	defer tx.rlock()()

	if tx.closed {
		return nil, ErrClosed
	}
	return scan(tx.me.Build().NomsMap(), opts)
}

// Put adds or updates an existing entry in the database.
func (tx *Transaction) Put(id string, json []byte) error {
	if tx.Closed() {
		return ErrClosed
	}

	value, err := nomsjson.FromJSON(json, tx.db.noms)
	if err != nil {
		return fmt.Errorf("could not Put '%s'='%s': %w", id, json, err)
	}

	defer tx.lock()()

	err = tx.me.Set(types.String(id), value)
	if err != nil {
		return fmt.Errorf("could not Put '%s'='%s': %w", id, value, err)
	}

	tx.wrote = true
	return nil
}

// Del removes an entry from the database. It returns true if the entry existed
// before the call to Del.
func (tx *Transaction) Del(id string) (ok bool, err error) {
	defer tx.lock()()

	if tx.closed {
		return false, ErrClosed
	}

	k := types.String(id)
	ok = tx.me.Has(k)
	if ok {
		err = tx.me.Remove(k)
		if err == nil {
			tx.wrote = true
		}
	}
	return ok, err
}

// Close the transaction without committing any possible changes done in this
// transaction.
func (tx *Transaction) Close() error {
	defer tx.lock()()

	if tx.closed {
		return ErrClosed
	}
	tx.closed = true
	return nil
}

// Commit tries to commits the changes made to the database in this transaction.
// If this returns without an error the commit succeeded and the new ref of the
// database head is returned.
func (tx *Transaction) Commit() (ref types.Ref, err error) {
	defer tx.lock()()

	if tx.closed {
		err = ErrClosed
		return
	}

	tx.closed = true

	if !tx.wrote {
		// No need to do anything.
		return
	}

	// Commmit.
	basis := tx.basis.Ref()

	newMap := tx.me.Build()
	newDataChecksum := newMap.NomsChecksum()
	newData := tx.db.noms.WriteValue(newMap.NomsMap())

	var commit Commit
	if tx.IsReplay() {
		// Ideally we'd do this check earlier but we don't want to have a constructor
		// that can fail. We have this check at the api level so this here is just extra
		// protection.
		err = ValidateReplayParams(*tx.original, tx.name, tx.args, tx.basis.NextMutationID())
		if err != nil {
			return
		}
		commit = makeReplayedLocal(tx.db.noms, basis, time.DateTime(), tx.basis.NextMutationID(), tx.name, tx.args, newData, newDataChecksum, (*tx.original).Ref())
		ref = tx.db.noms.WriteValue(commit.NomsStruct)
		return
	}

	commit = makeLocal(tx.db.noms, basis, time.DateTime(), tx.basis.NextMutationID(), tx.name, tx.args, newData, newDataChecksum)
	ref = tx.db.noms.WriteValue(commit.NomsStruct)
	err = tx.db.setHead(commit)
	if err == nil {
		return
	}
	if !errors.Is(err, datas.ErrMergeNeeded) && !errors.Is(err, datas.ErrOptimisticLockFailed) {
		log.Printf("Unexpected error from FastForward: %s", err)
	}
	err = NewCommitError(err)
	ref = types.Ref{}
	return
}

func ValidateReplayParams(original Commit, name string, args types.Value, mutationID uint64) error {
	if original.Type() != CommitTypeLocal {
		return fmt.Errorf("only local mutations can be replayed; %s is a %v", original.NomsStruct.Hash().String(), original.Type())
	}
	if name != original.Meta.Local.Name {
		return fmt.Errorf(`invalid replay: Names do not match, got "%s", expected "%s"`, name, original.Meta.Local.Name)
	}
	if !args.Equals(original.Meta.Local.Args) {
		return fmt.Errorf("invalid replay: Args do not match")
	}
	if mutationID != original.Meta.Local.MutationID {
		return fmt.Errorf("invalid replay: MutationID values do not match")
	}
	return nil
}
