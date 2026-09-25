# 001: The Leader Completeness check was stricter than the property

**Found:** Phase 5, `Kv.ChaosWithClientsNeverLosesOrDuplicatesAnAppend`, seed 47.
**Where the bug was:** the test harness (`tests/raft_cluster.hpp`), not Raft.
**Replay:** `RAFTKV_SEED=47 RAFTKV_DUMP_LOG=1 ./build/dev/raftkv_tests --gtest_filter='Kv.Chaos*'`
(against the commit before the fix).

## What the checker reported

```
t=5388607us leader 5 of term 12 lacks committed index 129
```

## What actually happened

| time | event |
|---|---|
| 3.17 s | node 5 starts an election for term 12 and sends `RequestVote` |
| 3.18 s | chaos pauses node 5; its vote replies queue up unread |
| 4.11 s | node 3, leader of term 16, commits index 129 |
| 5.39 s | node 5 resumes, reads the queued term-12 votes, becomes leader of term 12, then immediately sees term 16 and steps down |

Node 5 really did win term 12. Nobody else led term 12, so Election Safety held. It
lacked index 129 because index 129 was committed in term 16, *after* term 12.

## Why the check was wrong

The harness required a new leader to hold **every** entry committed so far. Figure 3 of the
paper states Leader Completeness more narrowly: an entry committed in term *t* is present in
the logs of leaders of all terms **greater than *t***. A stale leader of an older term is
allowed to lack entries committed in newer terms, and Raft makes it step down as soon as it
hears of them.

## Fix

The harness now records the term in which each index was committed (seen in the same event
in which a leader advances its commit index). A new leader of term *T* must hold every entry
committed in a term below *T*.

## Did the fix weaken the check?

Re-planting the bug that Leader Completeness exists to catch (granting votes without the
log up-to-date check) still fails 9 tests, including cluster-level chaos with
`leader 3 of term 4 lacks committed index 2`.

## Lesson

Write an invariant exactly as the paper states it, quantifiers included. "Every committed
entry" and "every entry committed in an earlier term" differ only once pauses and delayed
messages let a node act on old information, which is what the fault injector is for. It
took 50 seeds × 5 nodes with pauses to hit it, and 1,000+ earlier chaos runs never did.
