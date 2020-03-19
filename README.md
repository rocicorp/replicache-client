# Replicache Client

This is a shared native library used by all Replicache clients. Currently it is implemented by via of [GoMobile](https://godoc.org/golang.org/x/mobile/cmd/gomobile),
which means it is usable only by iOS an Android.

However, it should eventually also have a cgo target, so that it can be used by desktop clients too.

# Versioning

This library is not meant to be used directly and does not commit to a stable interface. Breaking changes
are introduced regularly whenever convenient.

Clients should use something higher level like `replicache-sdk-flutter`, which does provide a stable API.
Higher-level language/platform-specific bindings packages will statically link to a specific version of
this library and handle API changes when they update.

# Build

```
./build.sh
```

# Release

```
go tag v<newsemver>
go push <vnewsemver> origin
./release.sh
# Github automatically shows the new tag at https://github.com/rocicorp/replicache-client/releases
# Edit that release, add a description, and upload the objects
```
