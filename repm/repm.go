// Package repm implements an Android and iOS interface to Replicache via [Gomobile](https://github.com/golang/go/wiki/Mobile).
// repm is not thread-safe. Callers must guarantee that it is not called concurrently on different threads/goroutines.
package repm

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path"
	"runtime"
	"sync/atomic"

	"github.com/attic-labs/noms/go/spec"
	zl "github.com/rs/zerolog"
	zlog "github.com/rs/zerolog/log"

	"roci.dev/diff-server/util/chk"
	"roci.dev/diff-server/util/log"
	"roci.dev/diff-server/util/time"
	"roci.dev/diff-server/util/version"
	"roci.dev/replicache-client/db"

	// Log all http request/response pairs.
	_ "roci.dev/diff-server/util/loghttp"
)

var (
	connections = map[string]*connection{}
	repDir      string

	// Unique rpc request ID
	rid uint64
)

// Logger allows client to optionally provide a place to send repm's log messages.
type Logger interface {
	io.Writer
}

// Init initializes Replicache. If the specified storage directory doesn't exist, it
// is created. Logger receives logging output from Replicache.
func Init(storageDir, tempDir string, logger Logger) {
	if logger == nil {
		zlog.Logger = zlog.Output(zl.ConsoleWriter{Out: os.Stdout})
	} else {
		zlog.Logger = zlog.Output(zl.ConsoleWriter{Out: logger, NoColor: true})
	}

	l := log.Default()
	l.Info().Msg("Hello from repm")

	if storageDir == "" {
		l.Error().Msg("storageDir must be non-empty")
		return
	}
	if tempDir != "" {
		os.Setenv("TMPDIR", tempDir)
	}

	repDir = storageDir
}

// for testing
func deinit() {
	connections = map[string]*connection{}
	repDir = ""
}

// Dispatch send an API request to Replicache, JSON-serialized parameters, and returns the response.
func Dispatch(dbName, rpc string, data []byte) (ret []byte, err error) {
	t0 := time.Now()
	l := log.Default().With().
		Str("db", dbName).
		Str("req", rpc).
		Uint64("rid", atomic.AddUint64(&rid, 1)).
		Logger()

	l.Debug().Bytes("data", data).Msg("rpc -->")

	defer func() {
		t1 := time.Now()
		l.Debug().Bytes("ret", ret).Dur("dur", t1.Sub(t0)).Msg("rpc <--")
		if r := recover(); r != nil {
			var msg string
			if e, ok := r.(error); ok {
				msg = e.Error()
			} else {
				msg = fmt.Sprintf("%v", r)
			}
			l.Error().Stack().Msgf("Replicache panicked with: %s\n", msg)
			ret = nil
			err = fmt.Errorf("Replicache panicked with: %s - see stderr for more", msg)
		}
	}()

	switch rpc {
	case "list":
		return list(l)
	case "open":
		return nil, open(dbName, l)
	case "close":
		return nil, close(dbName)
	case "drop":
		return nil, drop(dbName)
	case "version":
		return []byte(version.Version()), nil
	case "profile":
		profile(l)
		return nil, nil
	case "setLogLevel":
		// dbName param is ignored
		level := string(data)
		switch level {
		case "debug":
			zl.SetGlobalLevel(zl.DebugLevel)
		case "info":
			zl.SetGlobalLevel(zl.InfoLevel)
		case "error":
			zl.SetGlobalLevel(zl.ErrorLevel)
		default:
			return nil, fmt.Errorf("Invalid level: %s", level)
		}
		return nil, nil
	}

	conn := connections[dbName]
	if conn == nil {
		return nil, errors.New("specified database is not open")
	}

	l = l.With().Str("cid", conn.db.ClientID()).Logger()

	switch rpc {
	case "getRoot":
		return conn.dispatchGetRoot(data)
	case "has":
		return conn.dispatchHas(data)
	case "get":
		return conn.dispatchGet(data)
	case "scan":
		return conn.dispatchScan(data)
	case "put":
		return conn.dispatchPut(data)
	case "del":
		return conn.dispatchDel(data)
	case "beginSync":
		return conn.dispatchBeginSync(data, l)
	case "maybeEndSync":
		return conn.dispatchMaybeEndSync(data)
	case "openTransaction":
		return conn.dispatchOpenTransaction(data)
	case "closeTransaction":
		return conn.dispatchCloseTransaction(data)
	case "commitTransaction":
		return conn.dispatchCommitTransaction(data, l)
	}
	chk.Fail("Unsupported rpc name: %s", rpc)
	return nil, nil
}

type DatabaseInfo struct {
	Name string `json:"name"`
}

type ListResponse struct {
	Databases []DatabaseInfo `json:"databases"`
}

func list(l zl.Logger) (resBytes []byte, err error) {
	if repDir == "" {
		return nil, errors.New("must call init first")
	}

	resp := ListResponse{
		Databases: []DatabaseInfo{},
	}

	fi, err := os.Stat(repDir)
	if err != nil {
		if os.IsNotExist(err) {
			return json.Marshal(resp)
		}
		return nil, err
	}
	if !fi.IsDir() {
		return nil, errors.New("Specified path is not a directory")
	}
	entries, err := ioutil.ReadDir(repDir)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			b, err := base64.RawURLEncoding.DecodeString(entry.Name())
			if err != nil {
				l.Err(err).Msgf("Could not decode directory name: %s, skipping", entry.Name())
				continue
			}
			resp.Databases = append(resp.Databases, DatabaseInfo{
				Name: string(b),
			})
		}
	}
	return json.Marshal(resp)
}

// Open a Replicache database. If the named database doesn't exist it is created.
func open(dbName string, l zl.Logger) error {
	if repDir == "" {
		return errors.New("Replicache is uninitialized - must call init first")
	}
	if dbName == "" {
		return errors.New("dbName must be non-empty")
	}

	if _, ok := connections[dbName]; ok {
		return nil
	}

	p := dbPath(repDir, dbName)
	l.Info().Msgf("Opening Replicache database '%s' at '%s'", dbName, p)
	l.Debug().Msgf("Using tempdir: %s", os.TempDir())
	sp, err := spec.ForDatabase(p)
	if err != nil {
		return err
	}
	db, err := db.Load(sp)
	if err != nil {
		return err
	}

	connections[dbName] = newConnection(db, p)
	return nil
}

// Close releases the resources held by the specified open database.
func close(dbName string) error {
	if dbName == "" {
		return errors.New("dbName must be non-empty")
	}
	conn := connections[dbName]
	if conn == nil {
		return nil
	}
	delete(connections, dbName)
	return nil
}

// Drop closes and deletes the specified local database. Remote replicas in the group are not affected.
func drop(dbName string) error {
	if repDir == "" {
		return errors.New("Replicache is uninitialized - must call init first")
	}
	if dbName == "" {
		return errors.New("dbName must be non-empty")
	}

	conn := connections[dbName]
	p := dbPath(repDir, dbName)
	if conn != nil {
		if conn.dir != p {
			return fmt.Errorf("open database %s has directory %s, which is different than specified %s",
				dbName, conn.dir, p)
		}
		close(dbName)
	}
	return os.RemoveAll(p)
}

func dbPath(root, name string) string {
	return path.Join(root, base64.RawURLEncoding.EncodeToString([]byte(name)))
}

func profile(l zl.Logger) {
	runtime.SetBlockProfileRate(1)
	go func() {
		l.Info().Msgf("Enabling http profiler: %s", http.ListenAndServe("localhost:6060", nil))
	}()
}
