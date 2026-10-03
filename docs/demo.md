# Recording the demo

A 2–3 minute narrated screen recording, following the plan's Phase 10 beats: one command
starts a 3-node cluster, a client writes continuously, the leader is killed on camera, writes
pause and resume, the checker reports the history linearizable, and one limitation is stated
out loud.

## Before recording

```sh
cmake --preset rel && cmake --build --preset rel   # tools/demo.sh uses build/rel
go version                                        # needed for the linearizability check
tools/demo.sh --check                             # dry run: should end with "demo: PASS"
```

Use a large terminal font, and a window about 100 columns wide so no line wraps. The run
itself takes about 25 seconds; everything else is narration over the README.

## The script

| Time | On screen | Say (roughly) |
|---|---|---|
| 0:00 | The README, top | "This is raftkv: a replicated key-value store I built in C++20 on the Raft consensus algorithm, from the paper. Three processes agree on every write, and it keeps working when one dies. The interesting part isn't the store; it's the evidence that it's correct." |
| 0:20 | Scroll to the architecture diagram | "The Raft core is single-threaded and never touches the OS directly: time, the network and the disk come in through one interface. In tests that's a deterministic simulator, so any failure replays from a seed. In production it's real TCP and real files. Same code in both." |
| 0:45 | Terminal: run `tools/demo.sh` | "One command starts three real server processes on this laptop." |
| 0:50 | Leader announced, load starts | "Four clients are now writing and reading continuously: about 115 operations a second, every write fsynced to disk." |
| 1:00 | The countdown, then `kill -9` | "Now I kill the leader with `kill -9`: no warning, no clean shutdown." |
| 1:05 | The dip at the next second, then recovery | "Throughput dips for about half a second while the other two elect a new leader, then it's back. It actually goes up: all three nodes share this one SSD, so two nodes means less disk contention. That wouldn't happen on separate machines." |
| 1:30 | "node N took over; longest pause …" | "Node N took over. The longest gap between completed operations was about 500 milliseconds." |
| 1:40 | Porcupine: "0 illegal" | "Now the real test. Every operation the clients made, about 2,500 of them with their start and end times, goes to Porcupine, a linearizability checker. It searches for a single order of operations that explains every result the clients saw. It found one: across a leader crash, no acknowledged write was lost and no read went back in time." |
| 2:05 | The limitation lines | "One limit, honestly. Every write waits for its own 4-millisecond fsync, so a node tops out near 250 writes a second; batching syncs together, group commit, is the obvious next step. And that half-second pause is mostly the client's 500 ms timeout, not the election." |
| 2:25 | README: fault matrix and results | "Everything here is automated: a fault matrix of crashes, partitions and corrupted disks, all replayable from a seed; 540 checked histories; and benchmarks whose predictions I wrote down before measuring. I got two of three wrong, and the README says so. Thanks for watching." |

## If something goes wrong on camera

- `demo: FAIL: the cluster never came up`: a port in 20000–31999 is taken. Run it again;
  the ports are random.
- Porcupine reports `ILLEGAL`: that would be a real bug. Keep the directory the failure
  message names; it contains the history and an HTML visualization.
