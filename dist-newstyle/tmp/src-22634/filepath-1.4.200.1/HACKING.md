# Hacking

Most of the code is in `System/FilePath/Internal.hs` which is `cpphs`'d into both `System/FilePath/Posix.hs`
and `System/FilePath/Windows.hs` via `make cpp` and commited to the repo. This Internal module is a bit weird
in that it isn't really a Haskell module, but is more an include file.

The library has extensive doc tests. Anything starting with `-- >` is transformed into a doc test as a predicate
that must evaluate to `True`. These tests follow a few rules:

* Tests prefixed with `Windows:` or `Posix:` are only tested against that specific
  implementation - otherwise tests are run against both implementations.
* Any single letter variable, e.g. `x`, is considered universal quantification, and is checked with `QuickCheck`.
* If `Valid x =>` appears at the start of a doc test, that means the property
  will only be tested with `x` passing the `isValid` predicate.

The tests can be generated by `make gen` in the root of the repo, and will be placed in `tests/filepath-tests/TestGen.hs`.
The `TestGen.hs` file is checked into the repo, and the CI scripts check that `TestGen.hs` is in sync with
what would be generated a fresh - if you don't regenerate `TestGen.hs` the CI will fail.
