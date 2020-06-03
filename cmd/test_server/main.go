package main

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"

	"gopkg.in/alecthomas/kingpin.v2"
	"roci.dev/diff-server/util/chk"
	"roci.dev/diff-server/util/log"
	"roci.dev/diff-server/util/time"

	"roci.dev/replicache-client/repm"
)

func main() {
	impl(os.Args[1:], os.Stdin, os.Stdout, os.Stderr, os.Exit)
}

func impl(args []string, in io.Reader, out, errs io.Writer, exit func(int)) {
	app := kingpin.New("test-server", "")
	app.ErrorWriter(errs)
	app.UsageWriter(errs)
	app.Terminate(exit)

	port := app.Flag("port", "The port to run on").Default("7002").Int()
	logLevel := app.Flag("log-level", "Log verbosity level").Default("info").Enum("error", "info", "debug")
	useFakeTime := app.Flag("fake-time", "Use a fake time for more stable commit hashes").Default("true").Bool()

	_, err := app.Parse(args)
	if err != nil {
		fmt.Fprintln(errs, err.Error())
		exit(1)
	}

	if *useFakeTime {
		defer time.SetFake()()
	}

	storageDir, err := ioutil.TempDir("", "")
	err = log.SetGlobalLevelFromString(*logLevel)
	chk.NoError(err)

	repm.Init(storageDir, "", nil)

	ps := fmt.Sprintf(":%d", *port)
	fmt.Printf("Listening on %s...\n", ps)
	var s http.Handler = testServer{repm.Dispatch}
	http.ListenAndServe(fmt.Sprintf(":%d", *port), s)
}

type testServer struct {
	dispatch func(dbName, rpc string, data []byte) (ret []byte, err error)
}

func (s testServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	fmt.Println(req.URL.String())
	if req.URL.Path == "/statusz" {
		w.Write([]byte("OK"))
		return
	}

	if req.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Access-Control-Allow-Origin", "*")

	// Use query params instead of body for the rpc and dbname to simplify the
	// Dart SDK tests. It might make more sense to put these params in a JSON
	// body in the future.
	rpc := req.URL.Query().Get("rpc")
	dbName := req.URL.Query().Get("dbname")

	body := bytes.Buffer{}
	_, err := io.Copy(&body, req.Body)
	if err != nil {
		serverError(w, http.StatusInternalServerError, fmt.Sprintf("Could not read body: %s", err))
		return
	}

	res, err := s.dispatch(dbName, rpc, body.Bytes())

	if err != nil {
		serverError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to dispatch %s: %s", rpc, err))
		return
	}

	w.Write(res)
}

func serverError(w http.ResponseWriter, statusCode int, message string) {
	w.WriteHeader(statusCode)
	w.Write([]byte(message))
}
