# Linearizability checking: a planted stale read

`planted-stale-read.html` is Porcupine's visualization of a history that is **not
linearizable**. Open it in a browser. The bug in it was **planted on purpose**. The checker
has not found a real linearizability bug in raftkv (see "Results" below), so this is a
demonstration of what the checker catches, not a postmortem.

## The planted bug

A leader answers `Get` from its own state machine instead of putting the read through the
Raft log:

```cpp
// kv::Server::handle_request, planted:
if (c.op == Op::Get) { reply(from, state_.get(c.key)); return; }
```

This looks harmless, since the leader has every committed write. It breaks when the leader
has been **deposed without knowing it**: cut off from the majority, it still believes it
leads while the others elect a new leader and keep writing. Its reads are then stale. This
is why raftkv puts reads through the log, and why `ReadIndex` and lease reads exist.

## What the checker found

Seed 61, cut-off variant (`planted-stale-read.json`), key `z`:

| client | invoked | returned | operation |
|---|---|---|---|
| 1 | 5.962 s | 7.485 s | `get("z") -> "104.30;102.56;"` |
| 2 | 7.506 s | 7.512 s | `get("z") -> "103.33;"` |

Client 2's read began 21 ms **after** client 1's read had finished, yet it returned an
older value, one that a put had already overwritten. No order of operations explains both
results. Porcupine places 51 of the 62 operations on key `z` and then fails. The stale value
came from a node still serving as leader of an old term.

Reproduce: re-plant the lines above in `src/kv/server.cpp`, then

```sh
RAFTKV_HISTORY_DIR=out ./build/rel/raftkv_tests --gtest_filter='Linearizability.*'
go run ./tools/lincheck -viz out out/*.json
```

## Results against raftkv as built

200 histories per run (100 seeds × {healed, cut-off}), 4 clients, 3 keys, 5 servers, random
crashes, partitions, pauses and message loss:

- **raftkv:** 82,539 operations (400 never returned): **all linearizable**.
- **Planted bugs**, each caught:

  | Planted bug | Histories flagged (of 200) |
  |---|---|
  | leader answers `Get` locally (this page) | 6 |
  | any server answers `Get` locally | 68 |
  | no duplicate detection | 157 |
  | reply to writes before they commit | 77 |

Prediction 5 in `raftkv.md` was that the checker would find a real bug the unit tests missed.
**So far it has not.** The one real bug found in this project (postmortem 001) came from the
invariant checker, and it was a bug in the checker itself.
