# 003: Deleting entries after a snapshot deleted nothing

**Found:** while refactoring `FileStorage` for the power-loss model, by reading the code.
No test had hit it.
**Where:** `src/store/file_storage.cpp`, `FileStorage::truncate_suffix`.
**Severity:** a node could permanently refuse to start after an ordinary leader change.

## The bug

```cpp
if (from > state_.log.size()) return;            // "nothing to delete"
...
state_.log.resize(static_cast<std::size_t>(from - 1));
```

It treated a position in the in-memory log as a log index. That was true until Phase 8 added
snapshots: after a snapshot at index 1000, the log holds entries 1001, 1002, … so its size
is no longer its last index.

## What would happen

1. A follower holds entries 1001–1010 (snapshot at 1000).
2. A new leader's entries conflict from 1005, so Raft asks storage to delete 1005 onward.
3. `1005 > size (10)`: the function returned early. No truncation marker was written.
4. The new entries 1005, 1006, … were appended after the stale 1005–1010.
5. On the next restart, recovery read "entry 1005 does not follow entry 1010", treated it
   as corruption, and refused to start.

## Why the tests missed it

It needs truncation **after** compaction: a leader change that leaves a follower with a
conflicting suffix, on a node that has snapshotted, followed by a restart. The storage tests
truncated only uncompacted logs. The chaos tests with snapshots run Raft, which keeps its
own log in memory, so the bad on-disk state only mattered on a restart, and no seed combined
all of the above.

## Fix and evidence

Delete by index (`erase_if(e.index >= from)`), and decide "nothing to delete" by the last
entry's index. The regression test `FileStorageSnapshot.TruncateAfterCompactionDeletesTheRightEntries`
reproduced the failure exactly ("log entry 8 does not follow entry 10") before the fix,
and passes after.

## Lesson

Phase 8 routed every log index in Raft through one function (`Raft::at`), so compaction
changed one place. Storage kept a second, independent log with its own index arithmetic,
and that copy was never audited. Every place that maps an index to a position needs the
same treatment, and needs a test that runs after compaction.
