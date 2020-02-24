# The `repl` CLI

The Replicache SDK includes a command-line program called `repl` which you can use to interactively inspect and
manipulate Replicache client databases from the terminal.

To install it, copy the binary for your platform [from the latest release](https://github.com/roci.dev/replicache-client/releases/latest) to someplace in your path and run it:

```
cp <sdk>/repl-darwin-amd64 ~/bin/repl
chmod u+x ~/bin/repl
```

## Interacting with Replicache databases

Example:

```
$ /tmp/bundle.js <<HERE
function createUser(id, name, favoriteColor) {
  db.put('user/' + id, {
    name: name,
    color: favoriteColor,
  });
}

function getUsersByColor(color) {
  return db.scan({prefix:'user/'})
    .filter(function(kv) { return kv.value.color == color })
    .map(function(kv) { return kv.value });
}
HERE

$ repl --db=/tmp/mydb exec --bundle=/tmp/bundle.js createUser `uuidgen` Abby orange
$ repl --db=/tmp/mydb exec --bundle=/tmp/bundle.js createUser `uuidgen` Aaron orange
$ repl --db=/tmp/mydb exec --bundle=/tmp/bundle.js createUser `uuidgen` Sam green

$ repl --db=/tmp/mydb exec --bundle=/tmp/bundle.js getUsersByColor orange
[
  map {
    "color": "orange",
    "name": "Abby",
  },
  map {
    "color": "orange",
    "name": "Aaron",
  },
]
```

See `repl --help` for complete documentation.

## Noms CLI

Replicache is internally built on top of [Noms](https://github.com/attic-labs/noms). This is an implementation detail that we don't intend to expose to users. But while Replicache is young, it can ocassionally be useful to dive down into the guts and see what's going on.

*** Warning! The Noms CLI is extremely user-unfriendly. This is not intended to be part of the long-term DX of Replicache, it's just a temporary stop-gap. ***

Use the Noms CLI the same as `repl` - just copy it out of the release and run it.

See the [Noms CLI Tour](https://github.com/attic-labs/noms/blob/master/doc/cli-tour.md) for an introduction to using the CLI.

Here are some starting commands that will be useful to Replicache users:

```
# Prints out the current local state of the database.
# You can also ask for "remote" which is analagous to Git's origin/master -- it is the last-known state of the remote.
noms show /path/to/mydb::local

# Page through the (local) history of the database.
# Useful flags to this:
# --show-value (show the entire value of each commit, not just the diff)
# --max-lines
# --graph
# ... etc ... see --help for more.
noms log /path/to/mydb::local

# Note: there are some bugs in `noms log` where some flag combinations can cause it to crash when paging through
# logs. This doesn't indicate bad data or a deeper problem, it's just a bug in `log` :(.

# Prints out the entire current key/value store
noms show /path/to/mydb::local.value.data@target

# Prints out just the current value of "foo".
# The allowed syntax for show is fairly rich. For details, see:
# https://github.com/attic-labs/noms/blob/master/doc/spelling.md
noms show '/path/to/mydb::local.value.data@target["foo"]'

# Prints out the value of "foo" at a particular commit.
noms show '/path/to/mydb::#ks3ug9d7bavt69g6hjlssgfp6mc4scl9.value.data@target["foo"]'

# Prints the diff between two local commits (or arbitrary paths).
noms diff /path/to/mydb::#ks3ug9d7bavt69g6hjlssgfp6mc4scl9 /path/to/mydb::local

# Prints the diff between commits on different databases
noms diff https://serve.replicate.to/mydb::local /path/to/mydb::local
```
