package main

import (
	"fmt"
	"io/ioutil"
	"regexp"
	"strings"
	"testing"

	"github.com/attic-labs/noms/go/spec"
	"github.com/stretchr/testify/assert"

	"roci.dev/diff-server/util/time"
	"roci.dev/replicache-client/db"
)

func TestCommands(t *testing.T) {
	assert := assert.New(t)
	defer time.SetFake()()

	td, err := ioutil.TempDir("", "")
	fmt.Println("test database:", td)
	assert.NoError(err)

	commitA := "commit 0edk63ktqf2m2oj9jrsge5mlk7bl39gp\nCreated:     2014-01-24 00:00:00 -1000 HST\nStatus:      PENDING\nMerged:      2014-01-24 00:00:00 -1000 HST\nTransaction: .putValue([\n  \"foo\",\n  \"bar\",\n])\n(root) {\n+   \"foo\": \"bar\"\n  }\n\n"
	commitB := "commit 4mim1hir8ss5cmq09v886k75h7ru19o9\nCreated:     2014-01-24 00:00:00 -1000 HST\nStatus:      PENDING\nMerged:      2014-01-24 00:00:00 -1000 HST\nTransaction: .delValue([\n  \"foo\",\n])\n(root) {\n-   \"foo\": \"bar\"\n  }\n\n"

	tc := []struct {
		label string
		in    string
		args  string
		code  int
		out   string
		err   string
	}{
		{
			"log empty",
			"",
			"log --no-pager",
			0,
			"",
			"",
		},
		{
			"put missing-key",
			"",
			"put",
			1,
			"",
			"required argument 'key' not provided\n",
		},
		{
			"exec missing-val",
			"",
			"put foo",
			1,
			"",
			"could not parse value \"\" as json: couldn't parse value '' as json: unexpected end of JSON input\n",
		},
		{
			"put good",
			"\"bar\"",
			"put foo",
			0,
			"",
			"",
		},
		{
			"log put good",
			"",
			"log --no-pager",
			0,
			commitA,
			"",
		},
		{
			"has missing-arg",
			"",
			"has",
			1,
			"",
			"required argument 'key' not provided\n",
		},
		{
			"has good",
			"",
			"has foo",
			0,
			"true\n",
			"",
		},
		{
			"get bad missing-arg",
			"",
			"get",
			1,
			"",
			"required argument 'id' not provided\n",
		},
		{
			"get good",
			"",
			"get foo",
			0,
			"\"bar\"",
			"",
		},
		{
			"scan all",
			"",
			"scan",
			0,
			"foo: \"bar\"\n",
			"",
		},
		{
			"scan prefix good",
			"",
			"scan --prefix=f",
			0,
			"foo: \"bar\"\n",
			"",
		},
		{
			"scan prefix bad",
			"",
			"scan --prefix=g",
			0,
			"",
			"",
		},
		{
			"scan start-id good",
			"",
			"scan --start-id=foo",
			0,
			"foo: \"bar\"\n",
			"",
		},
		{
			"scan start-id bad",
			"",
			"scan --start-id=g",
			0,
			"",
			"",
		},
		{
			"scan start-id-exclusive good",
			"",
			"scan --start-id=f --start-id-exclusive",
			0,
			"foo: \"bar\"\n",
			"",
		},
		{
			"scan start-id-exclusive bad",
			"",
			"scan --start-id=foo --start-id-exclusive",
			0,
			"",
			"",
		},
		{
			"scan start-index good",
			"",
			"scan --start-index=0",
			0,
			"foo: \"bar\"\n",
			"",
		},
		{
			"scan start-index bad",
			"",
			"scan --start-index=1",
			0,
			"",
			"",
		},
		{
			"del bad missing-arg",
			"",
			"del",
			1,
			"",
			"required argument 'id' not provided\n",
		},
		{
			"del good no-op",
			"",
			"del monkey",
			0,
			"No such id.\n",
			"",
		},
		{
			"del good",
			"",
			"del foo",
			0,
			"",
			"",
		},
		{
			"log del good",
			"",
			"log --no-pager",
			0,
			commitB + commitA,
			"",
		},
	}

	for _, c := range tc {
		ob := &strings.Builder{}
		eb := &strings.Builder{}
		code := 0
		args := append([]string{"--db=" + td}, strings.Split(c.args, " ")...)
		impl(args, strings.NewReader(c.in), ob, eb, func(c int) {
			code = c
		})

		assert.Equal(c.code, code, c.label)
		assert.Equal(c.out, ob.String(), c.label)

		ebs := eb.String()
		re := regexp.MustCompile("ClientID: (.){22}\n")
		if c.err == "" {
			assert.Regexp(re, ebs)
		}
		ebs = re.ReplaceAllLiteralString(eb.String(), "")
		assert.Equal(c.err, ebs, c.label)
	}
}

func TestDrop(t *testing.T) {
	assert := assert.New(t)
	tc := []struct {
		in      string
		errs    string
		deleted bool
	}{
		{"no\n", "", false},
		{"N\n", "", false},
		{"balls\n", "", false},
		{"n\n", "", false},
		{"windows\r\n", "", false},
		{"y\n", "", true},
		{"y\r\n", "", true},
	}

	for i, t := range tc {
		d, dir := db.LoadTempDB(assert)

		tx := d.NewTransaction()
		err := tx.Put("foo", []byte(`"bar"`))
		assert.NoError(err)
		_, err = tx.Commit()
		assert.NoError(err)

		tx = d.NewTransaction()
		val, err := tx.Get("foo")
		assert.NoError(err)
		assert.Equal(`"bar"`, string(val))
		_, err = tx.Commit()
		assert.NoError(err)

		desc := fmt.Sprintf("test case %d, input: %s", i, t.in)
		args := append([]string{"--db=" + dir, "drop"})
		out := strings.Builder{}
		errs := strings.Builder{}
		code := 0
		impl(args, strings.NewReader(t.in), &out, &errs, func(c int) { code = c })

		assert.Equal(dropWarning, out.String(), desc)
		assert.Equal(t.errs, errs.String(), desc)
		assert.Equal(0, code, desc)
		sp, err := spec.ForDatabase(dir)
		assert.NoError(err)
		noms := sp.GetDatabase()
		ds := noms.GetDataset(db.MASTER_DATASET)
		assert.Equal(!t.deleted, ds.HasHead())
	}
}

func TestEmptyInput(t *testing.T) {
	assert := assert.New(t)
	db.LoadTempDB(assert)
	var args []string

	// Just testing that they don't crash.
	// See https://github.com/aboodman/replicant/issues/120
	impl(args, strings.NewReader(""), ioutil.Discard, ioutil.Discard, func(_ int) {})
	args = []string{"--db=/tmp/foo"}
	impl(args, strings.NewReader(""), ioutil.Discard, ioutil.Discard, func(_ int) {})
}
