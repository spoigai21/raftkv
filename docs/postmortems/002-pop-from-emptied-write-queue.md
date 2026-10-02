# 002: A server crashed when a peer died mid-write

**Found:** Phase 7, by the done-gate's `ctest --repeat until-fail:10`. `cluster_smoke` failed
in a Release run. Over 20 Release runs it failed 5 times.
**Where:** `src/net/asio_env.cpp`, `Connection`, the real TCP transport. The simulator
never runs this code, so no simulated test could have found it.
**Severity:** a surviving server crashed when another server was `kill -9`'d. In a
3-node cluster, losing a second node loses the majority: the failure Raft exists to
survive, turned into an outage by the transport.

## Symptom

```
== 2. kill -9 the leader
raftkvctl: no answer within 10000 ms
FAIL: put after leader kill
smoke.sh: line 31: 75575 Segmentation fault: 11  "$RAFTKVD" --id 1 ...
```

The macOS crash report put the fault in `Connection::~Connection()`, destroying its
`std::deque<std::string>` of outgoing frames, which held garbage.

## Cause

A `Connection` keeps a queue of frames and writes the front one with `async_write`. Two
things went wrong when the peer died:

1. A small write finished, and its completion handler was queued.
2. Before that handler ran, the **read** side saw the peer's reset and called `close()`,
   which **cleared the queue**.
3. The write handler then ran with no error and called `queue_.pop_front()` on an empty
   deque. That is undefined behaviour, and it corrupted the deque, which crashed later
   when the connection was freed.

A second problem came from the same `queue_.clear()`: an in-flight `async_write` could
still be pointing into the frame being freed.

## Fix

- The write handler returns at once if the connection is already closed.
- `close()` no longer clears the queue. The frames are freed with the `Connection`, after
  the last handler that holds it has run.

## Evidence

- **Regression test:** `AsioEnv.PeerClosingWhileWritesAreInFlightIsSafe` keeps small
  writes completing while the other end is closed under it, 300 times. Before the fix it
  segfaulted in 5 of 5 runs, with the same backtrace. After it, 10 of 10 passed.
- **The real-process test:** 5 of 20 failures before the fix; 19 of 19 after. The 20th
  run was one I killed by hand while investigating a separate test problem.

## Why the sanitizer builds missed it

The ASan/UBSan repeat passed 10 of 10. The bug needs a successful write completion and a
read error to land together, which depends on timing, and the sanitizer build's timing
rarely lines up. Popping an empty libc++ deque also does not necessarily touch memory ASan
watches until later.

A **hardened standard library** catches it outright. With the bug put back and libc++
hardening on (`_LIBCPP_HARDENING_MODE_EXTENSIVE`), the regression test trapped in 3 of 3
runs, at the exact line, `asio_env.cpp:83`, the `pop_front` itself, instead of segfaulting
later in a destructor. The ASan/UBSan and TSan presets now build with libc++ hardening and
`_GLIBCXX_ASSERTIONS` (libstdc++, as used in Linux CI). Release stays unhardened so the
Phase 9 benchmarks measure the plain build.

## Still open

During this investigation, `cluster_smoke` hung twice in step 4: once under TSan on the
pre-fix code (no logs survived), and once in Release after the fix. In the Release case,
the node that should have refused to start on a corrupted log started normally, because
the corruption was not in the file: afterwards, every record verified. FileStorage only appends and cuts torn tails off the
end, so how the byte was lost is not understood. 40 more traced runs did not reproduce it.
The script now checks that its corruption actually landed, and every wait in it has a
deadline, so a recurrence fails within seconds, with logs, instead of hanging.

## Lesson

The simulator proves the protocol, not the plumbing. The transport needed its own tests
with real sockets, and it needed them repeated, because its bugs live in the order in
which completions happen to arrive.
