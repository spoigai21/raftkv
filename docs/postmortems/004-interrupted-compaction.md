# 004: A power cut mid-snapshot could stop a node from ever restarting

**Found:** by the power-loss model (`tests/power_loss_test.cpp`) on its first run, seed 5.
**Where:** `src/store/file_storage.cpp`, `FileStorage::open` (recovery).
**Severity:** after one badly timed power cut, a node could refuse to start on its *next*
restart, with no data actually damaged.

## The bug

`save_snapshot` works in two durable steps:

1. write the new snapshot file (temp file, fsync, rename, fsync the directory);
2. rewrite the log without the entries the snapshot covers, and swap it in the same way.

A crash between the steps leaves the new snapshot over the old log. Recovery handled that
in memory: it reads the old log and drops the entries the snapshot covers. But it left the
old log *file* as it was, and new entries were appended to it.

## What would happen

1. A follower's log holds entries 1–5. The leader sends a snapshot up to index 7.
2. Step 1 finishes; the power goes out before step 2.
3. The node restarts: snapshot at 7, log entries 1–5 on disk, all covered. In memory the log
   is empty, which is correct.
4. Entry 8 arrives and is appended to the log file, after entry 5.
5. On the next restart, recovery reads "entry 8 does not follow entry 5", treats it as
   corruption, and refuses to start.

## Why the tests missed it

`kill -9` cannot stop a process *between* two of its own system calls on purpose, and the
crash tests never happened to land there with a snapshot past the end of the log. The
simulator's `SimStorage` keeps a log in memory and has no second file to get out of step.
It took a model that can cut the power at *every* file operation.

## Fix and evidence

Recovery now finishes the interrupted compaction: if the log file still holds entries the
snapshot covers, `open` rewrites it (atomically, as step 2 would have). The regression test
`PowerLoss.AnInterruptedCompactionIsFinishedOnRecovery` cuts the power at exactly that
point; it failed with "log entry 8 does not follow entry 5" before the fix and passes after.
The randomized test then passed 400 seeds × 12 power cuts.

The same model caught a regression in the refactor that made it possible: the log file was
no longer created by `open`, so a `sync()` before the first append failed, and its name
was never made durable. `open` now creates it with an fsync of the directory.

## Lesson

A multi-step update needs a recovery rule for **every** point between its steps, not just
"before" and "after". Recovery that repairs the in-memory view but not the files leaves
the next restart to find the mess.
