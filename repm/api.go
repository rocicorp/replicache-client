package repm

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/attic-labs/noms/go/hash"
	zl "github.com/rs/zerolog"
	"roci.dev/diff-server/util/chk"
	jsnoms "roci.dev/diff-server/util/noms/json"
	"roci.dev/replicache-client/db"
)

type connection struct {
	dir                string
	db                 *db.DB
	transactions       map[int]*db.Transaction
	transactionCounter int
	transactionMutex   sync.RWMutex
}

func newConnection(d *db.DB, p string) *connection {
	return &connection{db: d, dir: p, transactions: map[int]*db.Transaction{}, transactionCounter: 1}
}

func (conn *connection) findTransaction(txID int) (*db.Transaction, error) {
	if txID == 0 {
		return nil, fmt.Errorf("Missing transaction ID")
	}
	conn.transactionMutex.RLock()
	defer conn.transactionMutex.RUnlock()

	if tx, ok := conn.transactions[txID]; ok {
		return tx, nil
	}
	return nil, fmt.Errorf("Invalid transaction ID: %d", txID)
}

func (conn *connection) removeTransaction(txID int) {
	conn.transactionMutex.Lock()
	defer conn.transactionMutex.Unlock()
	delete(conn.transactions, txID)
}

func (conn *connection) dispatchGetRoot(reqBytes []byte) ([]byte, error) {
	var req getRootRequest
	err := json.Unmarshal(reqBytes, &req)
	if err != nil {
		return nil, err
	}

	res := getRootResponse{
		Root: jsnoms.Hash{
			Hash: conn.db.HeadHash(),
		},
	}
	return mustMarshal(res), nil
}

func (conn *connection) dispatchHas(reqBytes []byte) ([]byte, error) {
	var req hasRequest
	err := json.Unmarshal(reqBytes, &req)
	if err != nil {
		return nil, err
	}

	tx, err := conn.findTransaction(req.TransactionID)
	if err != nil {
		return nil, err
	}
	ok, err := tx.Has(req.Key)
	if err != nil {
		return nil, err
	}
	res := hasResponse{
		Has: ok,
	}
	return mustMarshal(res), nil
}

func (conn *connection) dispatchGet(reqBytes []byte) ([]byte, error) {
	var req getRequest
	err := json.Unmarshal(reqBytes, &req)
	if err != nil {
		return nil, err
	}
	tx, err := conn.findTransaction(req.TransactionID)
	if err != nil {
		return nil, err
	}
	v, err := tx.Get(req.Key)
	if err != nil {
		return nil, err
	}
	res := getResponse{}
	if v != nil {
		res.Has = true
		res.Value = v
	}
	return mustMarshal(res), nil
}

func (conn *connection) dispatchScan(reqBytes []byte) ([]byte, error) {
	var req scanRequest
	err := json.Unmarshal(reqBytes, &req)
	if err != nil {
		return nil, err
	}
	tx, err := conn.findTransaction(req.TransactionID)
	if err != nil {
		return nil, err
	}
	var items scanResponse
	items, err = tx.Scan(db.ScanOptions(req.ScanOptions))
	if err != nil {
		return nil, err
	}
	return mustMarshal(items), nil
}

func (conn *connection) dispatchPut(reqBytes []byte) ([]byte, error) {
	var req putRequest
	err := json.Unmarshal(reqBytes, &req)
	if err != nil {
		return nil, err
	}
	if len(req.Value) == 0 {
		return nil, errors.New("value field is required")
	}
	tx, err := conn.findTransaction(req.TransactionID)
	if err != nil {
		return nil, err
	}
	err = tx.Put(req.Key, req.Value)
	if err != nil {
		return nil, err
	}
	res := putResponse{}
	return mustMarshal(res), nil
}

func (conn *connection) dispatchDel(reqBytes []byte) ([]byte, error) {
	var req delRequest
	err := json.Unmarshal(reqBytes, &req)
	if err != nil {
		return nil, err
	}
	tx, err := conn.findTransaction(req.TransactionID)
	if err != nil {
		return nil, err
	}
	ok, err := tx.Del(req.Key)
	if err != nil {
		return nil, err
	}
	res := delResponse{
		Ok: ok,
	}
	return mustMarshal(res), nil
}

func (conn *connection) dispatchBeginSync(reqBytes []byte, l zl.Logger) ([]byte, error) {
	var req beginSyncRequest
	err := json.Unmarshal(reqBytes, &req)
	if err != nil {
		return nil, err
	}
	syncHead, syncInfo, err := conn.db.BeginSync(req.BatchPushURL, req.DiffServerURL, req.DiffServerAuth, req.DataLayerAuth, l)
	if err != nil {
		return nil, fmt.Errorf("sync %s failed: %w", syncInfo.SyncID, err)
	}
	res := beginSyncResponse{
		SyncHead: jsnoms.Hash{Hash: syncHead},
		SyncInfo: syncInfo,
	}
	return mustMarshal(res), nil
}

func (conn *connection) dispatchMaybeEndSync(reqBytes []byte) ([]byte, error) {
	var req maybeEndSyncRequest
	err := json.Unmarshal(reqBytes, &req)
	if err != nil {
		return nil, err
	}
	replay, err := conn.db.MaybeEndSync(req.SyncHead.Hash, req.SyncID)
	if err != nil {
		return nil, fmt.Errorf("sync %s failed: %w", req.SyncID, err)
	}
	res := maybeEndSyncResponse{
		ReplayMutations: replay,
	}
	return mustMarshal(res), nil
}

func (conn *connection) newTransaction(name string, jsonArgs json.RawMessage, basis hash.Hash, original hash.Hash) (int, error) {
	conn.transactionMutex.Lock()
	defer conn.transactionMutex.Unlock()
	txID := conn.transactionCounter
	conn.transactionCounter++
	var tx *db.Transaction

	if name == "" && len(jsonArgs) == 0 {
		tx = conn.db.NewTransaction()
	} else {
		nomsArgs, err := jsnoms.FromJSON(jsonArgs, conn.db.Noms())
		if err != nil {
			return 0, err
		}
		var basisCommit, originalCommit *db.Commit
		// If it's a replay...
		if !basis.IsEmpty() {
			b, err := db.ReadCommit(conn.db.Noms(), basis)
			if err != nil {
				return 0, err
			}
			basisCommit = &b
			o, err := db.ReadCommit(conn.db.Noms(), original)
			if err != nil {
				return 0, err
			}
			originalCommit = &o
			if err := db.ValidateReplayParams(*originalCommit, name, nomsArgs, basisCommit.NextMutationID()); err != nil {
				return 0, err
			}
		}

		tx = conn.db.NewTransactionWithArgs(name, nomsArgs, basisCommit, originalCommit)
	}

	conn.transactions[txID] = tx
	return txID, nil
}

func (conn *connection) dispatchOpenTransaction(reqBytes []byte) ([]byte, error) {
	var req openTransactionRequest
	err := json.Unmarshal(reqBytes, &req)
	if err != nil {
		return nil, err
	}

	var basis, original hash.Hash
	if req.RebaseOpts != (rebaseOpts{}) {
		basis = req.RebaseOpts.Basis.Hash
		original = req.RebaseOpts.Original.Hash
	}

	txID, err := conn.newTransaction(req.Name, req.Args, basis, original)
	if err != nil {
		return nil, err
	}

	res := openTransactionResponse{
		TransactionID: txID,
	}
	return mustMarshal(res), nil
}

func (conn *connection) dispatchCloseTransaction(reqBytes []byte) ([]byte, error) {
	var req closeTransactionRequest
	err := json.Unmarshal(reqBytes, &req)
	if err != nil {
		return nil, err
	}
	tx, err := conn.findTransaction(req.TransactionID)
	if err != nil {
		return nil, err
	}
	conn.removeTransaction(req.TransactionID)
	err = tx.Close()
	if err != nil {
		return nil, err
	}
	res := closeTransactionResponse{}
	return mustMarshal(res), nil
}

func (conn *connection) dispatchCommitTransaction(reqBytes []byte, l zl.Logger) ([]byte, error) {
	var req commitTransactionRequest
	err := json.Unmarshal(reqBytes, &req)
	if err != nil {
		return nil, err
	}
	tx, err := conn.findTransaction(req.TransactionID)
	if err != nil {
		return nil, err
	}
	conn.removeTransaction(req.TransactionID)
	commitRef, err := tx.Commit(l)

	res := commitTransactionResponse{}

	if err == nil {
		res.Ref = &jsnoms.Hash{
			Hash: commitRef.TargetHash(),
		}
	} else {
		var commitErr db.CommitError
		if !errors.As(err, &commitErr) {
			return nil, err
		}
		res.RetryCommit = true
	}

	return mustMarshal(res), nil
}

func mustMarshal(thing interface{}) []byte {
	data, err := json.Marshal(thing)
	chk.NoError(err)
	return data
}
