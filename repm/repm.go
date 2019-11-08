// Package repm implements an Android and iOS interface to Replicant via [Gomobile](https://github.com/golang/go/wiki/Mobile).
// repm is not thread-safe. Callers must guarantee that it is not called concurrently on different threads/goroutines.
package repm

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"runtime/debug"

	"github.com/attic-labs/noms/go/spec"

	"roci.dev/replicant/api"
	"roci.dev/replicant/db"
	rlog "roci.dev/replicant/util/log"
	"roci.dev/replicant/util/version"
)

var (
	connections = map[string]*connection{}
	repDir      string
)

type connection struct {
	api *api.API
	dir string
}

// Logger allows client to optionally provide a place to send repm's log messages.
type Logger interface {
	io.Writer
}

// Init initializes Replicant. If the specified storage directory doesn't exist, it
// is created. Logger receives logging output from Replicant.
func Init(storageDir, tempDir string, logger Logger) {
	if logger == nil {
		logger = os.Stderr
	}
	rlog.Init(logger, rlog.Options{Prefix: true})

	if storageDir == "" {
		log.Print("storageDir must be non-empty")
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

// Dispatch send an API request to Replicant, JSON-serialized parameters, and returns the response.
// For the list of supported API requests and their parameters, see the api package.
func Dispatch(dbName, rpc string, data []byte) (ret []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			var msg string
			if e, ok := r.(error); ok {
				msg = e.Error()
			} else {
				msg = fmt.Sprintf("%v", r)
			}
			log.Printf("Replicant panicked with: %s\n%s\n", msg, string(debug.Stack()))
			ret = nil
			err = fmt.Errorf("Replicant panicked with: %s - see stderr for more", msg)
		}
	}()

	switch rpc {
	case "list":
		return list()
	case "open":
		return nil, open(dbName)
	case "close":
		return nil, close(dbName)
	case "drop":
		return nil, drop(dbName)
	case "version":
		return []byte(version.Version()), nil
	default:
		conn := connections[dbName]
		if conn == nil {
			return nil, errors.New("specified database is not open")
		}
		return conn.api.Dispatch(rpc, data)
	}
}

type DatabaseInfo struct {
	Name string `json:"name"`
}

type ListResponse struct {
	Databases []DatabaseInfo `json:"databases"`
}

func list() (resBytes []byte, err error) {
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
				log.Printf("Could not decode directory name: %s, skipping", entry.Name())
				continue
			}
			resp.Databases = append(resp.Databases, DatabaseInfo{
				Name: string(b),
			})
		}
	}
	return json.Marshal(resp)
}

// Open a replicant database. If the named database doesn't exist it is created.
func open(dbName string) error {
	if repDir == "" {
		return errors.New("Replicant is uninitialized - must call init first")
	}
	if dbName == "" {
		return errors.New("dbName must be non-empty")
	}

	if _, ok := connections[dbName]; ok {
		return nil
	}

	p := dbPath(repDir, dbName)
	log.Printf("Opening Replicant database '%s' at '%s'", dbName, p)
	log.Println("Using tempdir: ", os.TempDir())
	sp, err := spec.ForDatabase(p)
	if err != nil {
		return err
	}
	origin, err := initClientID(sp.GetDatabase())
	db, err := db.Load(sp, origin)
	if err != nil {
		return err
	}

	connections[dbName] = &connection{api: api.New(db), dir: p}
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

type dropRequest struct {
	ReplicantRootDir string `json:"replicantRootDir"`
}

// Drop closes and deletes the specified local database. Remote replicas in the group are not affected.
func drop(dbName string) error {
	if repDir == "" {
		return errors.New("Replicant is uninitialized - must call init first")
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
