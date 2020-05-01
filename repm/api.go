package repm

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"

	"roci.dev/diff-server/util/chk"
	jsnoms "roci.dev/diff-server/util/noms/json"
	"roci.dev/replicache-client/db"
)

type connection struct {
	dir                string
	db                 *db.DB
	sp                 pullProgress
	pulling            int32
	transactions       map[int]*db.Transaction
	transactionCounter int
	transactionMutex   sync.RWMutex
}

func newConnection(d *db.DB, p string) *connection {
	return &connection{db: d, dir: p, transactions: map[int]*db.Transaction{}, transactionCounter: 1}
}

type pullProgress struct {
	bytesReceived uint64
	bytesExpected uint64
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
			Hash: conn.db.Hash(),
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
	items, err := tx.Scan(db.ScanOptions(req.ScanOptions))
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

func (conn *connection) dispatchBeginSync(reqBytes []byte) ([]byte, error) {
	var req beginSyncRequest
	err := json.Unmarshal(reqBytes, &req)
	if err != nil {
		return nil, err
	}
	syncHead, syncInfo, err := conn.db.BeginSync(req.BatchPushURL, req.ClientViewURL, req.DataLayerAuth)
	if err != nil {
		return nil, err
	}
	res := beginSyncResponse{
		SyncHead: jsnoms.Hash{Hash: syncHead},
		SyncInfo: syncInfo,
	}
	if syncInfo.BatchPushInfo.HTTPStatusCode == http.StatusUnauthorized {
		res.PushError = &syncError{
			BadAuth: syncInfo.BatchPushInfo.ErrorMessage,
		}
	}
	if syncInfo.ClientViewInfo.HTTPStatusCode == http.StatusUnauthorized {
		res.PullError = &syncError{
			BadAuth: syncInfo.ClientViewInfo.ErrorMessage,
		}
	}

	return mustMarshal(res), nil
}

func (conn *connection) dispatchMaybeEndSync(reqBytes []byte) ([]byte, error) {
	var req maybeEndSyncRequest
	err := json.Unmarshal(reqBytes, &req)
	if err != nil {
		return nil, err
	}
	ended, replay, err := conn.db.MaybeEndSync(req.SyncHead.Hash)
	if err != nil {
		return nil, err
	}
	res := maybeEndSyncResponse{
		Ended:            ended,
	    ReplayMutations: replay,
	}
	return mustMarshal(res), nil
}

func (conn *connection) dispatchPull(reqBytes []byte) ([]byte, error) {
	var req pullRequest
	err := json.Unmarshal(reqBytes, &req)
	if err != nil {
		return nil, err
	}

	if !atomic.CompareAndSwapInt32(&conn.pulling, 0, 1) {
		return nil, errors.New("There is already a pull in progress")
	}

	defer chk.True(atomic.CompareAndSwapInt32(&conn.pulling, 1, 0), "UNEXPECTED STATE: Overlapping pulls somehow!")

	res := pullResponse{}
	clientViewInfo, err := conn.db.Pull(req.Remote.Spec, req.ClientViewAuth, func(received, expected uint64) {
		conn.sp = pullProgress{
			bytesReceived: received,
			bytesExpected: expected,
		}
	})
	if err != nil {
		return nil, err
	}
	res.Root = jsnoms.Hash{
		Hash: conn.db.Hash(),
	}

	if clientViewInfo.HTTPStatusCode == http.StatusUnauthorized {
		res.Error = &pullResponseError{
			BadAuth: clientViewInfo.ErrorMessage,
		}
	}

	return mustMarshal(res), nil
}

func (conn *connection) dispatchPullProgress(reqBytes []byte) ([]byte, error) {
	var req pullProgressRequest
	err := json.Unmarshal(reqBytes, &req)
	if err != nil {
		return nil, err
	}
	res := pullProgressResponse{
		BytesReceived: conn.sp.bytesReceived,
		BytesExpected: conn.sp.bytesExpected,
	}
	return mustMarshal(res), nil
}

func (conn *connection) newTransaction(name string, jsonArgs json.RawMessage) (int, error) {
	conn.transactionMutex.Lock()
	defer conn.transactionMutex.Unlock()
	txID := conn.transactionCounter
	conn.transactionCounter++
	var tx *db.Transaction

	if name == "" && len(jsonArgs) == 0 {
		tx = conn.db.NewTransaction()
	} else {
		nomsArgs, err := jsnoms.FromJSON(bytes.NewReader(jsonArgs), conn.db.Noms())
		if err != nil {
			return 0, err
		}
		tx = conn.db.NewTransactionWithArgs(name, nomsArgs)
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

	txID, err := conn.newTransaction(req.Name, req.Args)
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

func (conn *connection) dispatchCommitTransaction(reqBytes []byte) ([]byte, error) {
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
	commitRef, err := tx.Commit()
	if err != nil {
		return nil, err
	}
	res := commitTransactionResponse{}
	if !commitRef.IsZeroValue() {
		res.Ref = &jsnoms.Hash{
			Hash: commitRef.Hash(),
		}
	}
	return mustMarshal(res), nil
}

func mustMarshal(thing interface{}) []byte {
	data, err := json.Marshal(thing)
	chk.NoError(err)
	return data
}
