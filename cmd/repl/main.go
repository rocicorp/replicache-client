package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime/pprof"
	"runtime/trace"
	"strings"
	"syscall"
	"time"

	"github.com/attic-labs/noms/go/diff"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/outputpager"
	"github.com/mgutz/ansi"
	kingpin "gopkg.in/alecthomas/kingpin.v2"

	"roci.dev/diff-server/util/chk"
	"roci.dev/diff-server/util/kp"
	rlog "roci.dev/diff-server/util/log"
	"roci.dev/diff-server/util/noms/json"
	"roci.dev/diff-server/util/tbl"
	rtime "roci.dev/diff-server/util/time"
	"roci.dev/diff-server/util/version"
	"roci.dev/replicache-client/db"
)

const (
	dropWarning = "This command deletes an entire database and its history. This operations is not recoverable. Proceed? y/n\n"
)

type opt struct {
	Args     []string
	OutField string
}

func main() {
	impl(os.Args[1:], os.Stdin, os.Stdout, os.Stderr, os.Exit)
}

func impl(args []string, in io.Reader, out, errs io.Writer, exit func(int)) {
	app := kingpin.New("repl", "Command-Line Replicache Client")
	app.ErrorWriter(errs)
	app.UsageWriter(errs)
	app.Terminate(exit)

	v := app.Flag("version", "Prints the version of this client - same as the 'version' command.").Short('v').Bool()
	auth := app.Flag("auth", "The authorization token to pass to db when connecting.").String()
	sps := app.Flag("db", "The database to connect to. Both local and remote databases are supported. For local databases, specify a directory path to store the database in. For remote databases, specify the http(s) URL to the database (usually https://serve.replicache.dev/<mydb>).").PlaceHolder("/path/to/db").Required().String()
	tf := app.Flag("trace", "Name of a file to write a trace to").OpenFile(os.O_RDWR|os.O_CREATE, 0644)
	cpu := app.Flag("cpu", "Name of file to write CPU profile to").OpenFile(os.O_RDWR|os.O_CREATE, 0644)

	var sp *spec.Spec
	getSpec := func() (spec.Spec, error) {
		if sp != nil {
			return *sp, nil
		}
		s, err := spec.ForDatabase(*sps)
		if err != nil {
			return spec.Spec{}, err
		}
		s.Options.Authorization = *auth
		return s, nil
	}

	var rdb *db.DB
	getDB := func() (db.DB, error) {
		if rdb != nil {
			return *rdb, nil
		}
		sp, err := getSpec()
		if err != nil {
			return db.DB{}, err
		}
		r, err := db.Load(sp)
		if err != nil {
			return db.DB{}, err
		}
		rdb = r
		return *r, nil
	}
	app.PreAction(func(pc *kingpin.ParseContext) error {
		if *v {
			fmt.Println(version.Version())
			exit(0)
		}
		return nil
	})

	stopCPUProfile := func() {
		if *cpu != nil {
			pprof.StopCPUProfile()
		}
	}
	stopTrace := func() {
		if *tf != nil {
			trace.Stop()
		}
	}
	defer stopTrace()
	defer stopCPUProfile()

	app.Action(func(pc *kingpin.ParseContext) error {
		if pc.SelectedCommand == nil {
			return nil
		}

		// Init logging
		logOptions := rlog.Options{}
		rlog.Init(errs, logOptions)

		if *tf != nil {
			err := trace.Start(*tf)
			if err != nil {
				return err
			}
		}
		if *cpu != nil {
			err := pprof.StartCPUProfile(*tf)
			if err != nil {
				return err
			}
		}
		c := make(chan os.Signal)
		signal.Notify(c, os.Interrupt, syscall.SIGTERM)
		go func() {
			<-c
			stopTrace()
			stopCPUProfile()
			os.Exit(1)
		}()

		return nil
	})

	has(app, getDB, out)
	get(app, getDB, out)
	scan(app, getDB, out, errs)
	put(app, getDB, in)
	del(app, getDB, out)
	sync(app, getDB)
	drop(app, getSpec, in, out)
	logCmd(app, getDB, out)

	if len(args) == 0 {
		app.Usage(args)
		return
	}

	_, err := app.Parse(args)
	if err != nil {
		fmt.Fprintln(errs, err.Error())
		exit(1)
	}
}

type gdb func() (db.DB, error)
type gsp func() (spec.Spec, error)

func has(parent *kingpin.Application, gdb gdb, out io.Writer) {
	kc := parent.Command("has", "Check whether a key exists in the database.")
	id := kc.Arg("key", "key of the value to check for").Required().String()
	kc.Action(func(_ *kingpin.ParseContext) error {
		db, err := gdb()
		if err != nil {
			return err
		}
		tx := db.NewTransaction()
		defer tx.Close()
		ok, err := tx.Has(*id)
		if err != nil {
			return err
		}
		if ok {
			out.Write([]byte("true\n"))
		} else {
			out.Write([]byte("false\n"))
		}
		return nil
	})
}

func get(parent *kingpin.Application, gdb gdb, out io.Writer) {
	kc := parent.Command("get", "Reads a value from the database.")
	id := kc.Arg("id", "id of the value to get").Required().String()
	kc.Action(func(_ *kingpin.ParseContext) error {
		db, err := gdb()
		if err != nil {
			return err
		}
		tx := db.NewTransaction()
		defer tx.Close()
		v, err := tx.Get(*id)
		if err != nil {
			return err
		}
		if v == nil {
			return nil
		}
		_, err = out.Write(v)
		return err
	})
}

func scan(parent *kingpin.Application, gdb gdb, out, errs io.Writer) {
	kc := parent.Command("scan", "Scans values in-order from the database.")
	opts := db.ScanOptions{
		Start: &db.ScanBound{
			ID:    &db.ScanID{},
			Index: new(uint64),
		},
	}
	kc.Flag("prefix", "prefix of values to return").StringVar(&opts.Prefix)
	kc.Flag("start-id", "id of the value to start scanning at").StringVar(&opts.Start.ID.Value)
	kc.Flag("start-id-exclusive", "id of the value to start scanning at").BoolVar(&opts.Start.ID.Exclusive)
	kc.Flag("start-index", "id of the value to start scanning at").Uint64Var(opts.Start.Index)
	kc.Flag("limit", "maximum number of items to return").IntVar(&opts.Limit)
	kc.Action(func(_ *kingpin.ParseContext) error {
		db, err := gdb()
		if err != nil {
			return err
		}
		tx := db.NewTransaction()
		defer tx.Close()
		items, err := tx.Scan(opts)
		if err != nil {
			fmt.Fprintln(errs, err)
			return nil
		}
		for _, it := range items {
			fmt.Fprintf(out, "%s: %s\n", it.Key, types.EncodedValue(it.Value.Value))
		}
		return nil
	})
}

func put(parent *kingpin.Application, gdb gdb, in io.Reader) {
	kc := parent.Command("put", "Reads a JSON-formated value from stdin and puts it into the database.")
	id := kc.Arg("key", "key of the value to put").Required().String()
	kc.Action(func(_ *kingpin.ParseContext) error {
		db, err := gdb()
		if err != nil {
			return err
		}
		var v bytes.Buffer
		if _, err := v.ReadFrom(in); err != nil {
			return err
		}

		data := v.Bytes()
		val, err := json.FromJSON(bytes.NewReader(data), db.Noms())
		if err != nil {
			return fmt.Errorf("could not parse value \"%s\" as json: %s", data, err)
		}
		args := types.NewList(db.Noms(), types.String(*id), val)
		tx := db.NewTransactionWithArgs(".putValue", args)

		err = tx.Put(*id, data)
		if err == nil {
			_, err = tx.Commit()
		} else {
			tx.Close()
		}
		return err
	})
}

func del(parent *kingpin.Application, gdb gdb, out io.Writer) {
	kc := parent.Command("del", "Deletes an item from the database.")
	id := kc.Arg("id", "id of the value to delete").Required().String()
	kc.Action(func(_ *kingpin.ParseContext) error {
		db, err := gdb()
		if err != nil {
			return err
		}
		args := types.NewList(db.Noms(), types.String(*id))
		tx := db.NewTransactionWithArgs(".delValue", args)

		ok, err := tx.Del(*id)
		if err == nil && !ok {
			out.Write([]byte("No such id.\n"))
		}
		if err == nil {
			_, err = tx.Commit()
		} else {
			tx.Close()
		}
		return err
	})
}

func sync(parent *kingpin.Application, gdb gdb) {
	kc := parent.Command("sync", "Sync with a this client server.")
	clientViewAuth := kc.Flag("client-view-auth", "Client view authorization sent to the data layer.").Default("").String()
	remoteSpec := kp.DatabaseSpec(kc.Arg("remote", "Server to sync with. See https://github.com/attic-labs/noms/blob/master/doc/spelling.md#spelling-databases.").Required())

	kc.Action(func(_ *kingpin.ParseContext) error {
		db, err := gdb()
		if err != nil {
			return err
		}

		// TODO: progress
		_, err = db.Pull(*remoteSpec, *clientViewAuth, nil)
		return err
	})
}

func drop(parent *kingpin.Application, gsp gsp, in io.Reader, out io.Writer) {
	kc := parent.Command("drop", "Deletes a this client database and its history.")

	r := bufio.NewReader(in)
	w := bufio.NewWriter(out)
	kc.Action(func(_ *kingpin.ParseContext) error {
		w.WriteString(dropWarning)
		w.Flush()
		answer, err := r.ReadString('\n')
		if err != nil {
			return err
		}
		answer = strings.TrimSpace(answer)
		if answer != "y" {
			return nil
		}
		sp, err := gsp()
		if err != nil {
			return err
		}
		noms := sp.GetDatabase()
		_, err = noms.Delete(noms.GetDataset(db.LOCAL_DATASET))
		return err
	})
}

func logCmd(parent *kingpin.Application, gdb gdb, out io.Writer) {
	kc := parent.Command("log", "Displays the history of a this client database.")
	np := kc.Flag("no-pager", "supress paging functionality").Bool()

	kc.Action(func(_ *kingpin.ParseContext) error {
		d, err := gdb()
		if err != nil {
			return err
		}
		c := d.Head()
		r, err := d.RemoteHead()
		if err != nil {
			return err
		}
		inRemote := false

		if !*np {
			pgr := outputpager.Start()
			defer pgr.Stop()
			out = pgr.Writer
		}

		for {
			if c.Type() == db.CommitTypeGenesis {
				break
			}

			if c.Original.Equals(r.Original) {
				inRemote = true
			}

			initialCommit, err := c.InitalCommit(d.Noms())

			getStatus := func() (r string, mergedTime time.Time) {
				if inRemote {
					r = "MERGED"
				} else {
					r = "PENDING"
				}

				switch c.Type() {
				case db.CommitTypeReorder:
					r += " (REBASE)"
					mergedTime = c.Meta.Reorder.Date.Time
				case db.CommitTypeTx:
					mergedTime = c.Meta.Tx.Date.Time
				default:
					chk.Fail("unexpected commit type")
				}

				return
			}

			getTx := func() string {
				return fmt.Sprintf("%s(%s)", initialCommit.Meta.Tx.Name, types.EncodedValue(initialCommit.Meta.Tx.Args))
			}

			fmt.Fprintln(out, color("commit "+c.Original.Hash().String(), "red+h"))
			table := (&tbl.Table{}).
				Add("Created: ", rtime.String(initialCommit.Meta.Tx.Date.Time))

			status, t := getStatus()
			table.Add("Status: ", status)
			if t != (time.Time{}) {
				table.Add("Merged: ", rtime.String(t))
			}

			if !initialCommit.Original.Equals(c.Original) {
				initialBasis, err := initialCommit.Basis(d.Noms())
				if err != nil {
					return err
				}
				table.Add("Initial Basis: ", initialBasis.Original.Hash().String())
			}
			table.Add("Transaction: ", getTx())

			_, err = table.WriteTo(out)
			if err != nil {
				return err
			}

			basis, err := c.Basis(d.Noms())
			if err != nil {
				return err
			}

			err = diff.PrintDiff(out, basis.Data(d.Noms()).NomsMap(), c.Data(d.Noms()).NomsMap(), false)
			if err != nil {
				return err
			}

			fmt.Fprintln(out, "")
			c = basis
		}

		return nil
	})
}

func color(text, color string) string {
	if outputpager.IsStdoutTty() {
		return ansi.Color(text, color)
	}
	return text
}
