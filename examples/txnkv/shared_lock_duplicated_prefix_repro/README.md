# shared_lock_duplicated_prefix_repro

This directory contains a repo-local reproducer for the `SharedLockInfos` API
v2 double-prefix bug and the follow-up `TableError(OutOfOrder(...))` flush
failure on the `release-nextgen-202603` stack.

It is intentionally built on top of `client-go` test probes:

- `tikv.StoreProbe`
- `transaction.TxnProbe`
- `transaction.CommitterProbe`

Treat it as a debugging tool, not a public API example.

## Modes

- `warmup`
  Uses table-row-shaped child/parent keys with different synthetic table ids and
  stops before flush. This seeds the target shard with a foreign-key-like
  pattern.
- `repro`
  Uses raw `x...` child/parent keys, forces the malformed shared-lock rollback,
  and triggers `/flush`.

## Requirements

- Start a fresh nextgen cluster from matching `release-nextgen-202603`
  worktrees.
- Export `TIDB_X_DIR` to the data directory printed by `start-tidb-x`.
- The cluster should expose PD on `127.0.0.1:2379` and the TiKV status server on
  `127.0.0.1:20180`.

## Run

Use the wrapper script after exporting `TIDB_X_DIR`:

```bash
export TIDB_X_DIR=/tmp/tidb-x.xxxxx
./run_repro.sh
```

Or run manually:

```bash
GOWORK=off go run . -mode warmup
GOWORK=off go run . -mode repro
```

If you already have all Go dependencies cached locally and want to avoid network
access, you can additionally set `GOPROXY=off GOSUMDB=off`.

## Success signals

The program prints lines like:

```text
inner shared lock: type=Lock key=78fffffe...
codec.EncodeKey(inner.Key)=78fffffe78fffffe...
```

Semantically, the reproducer models:

- a child-row write protected by an exclusive lock
- a parent-row FK check protected by a shared lock
- a conflicting transaction that later tries to rewrite the parent row

`run_repro.sh` then checks TiKV logs for the first
`TableError(OutOfOrder(...))` line and stops the repro phase immediately once it
appears.

The wrapper assumes you are running against a fresh cluster. If you reuse a
cluster that already contains old `OutOfOrder` logs, clear it first.
