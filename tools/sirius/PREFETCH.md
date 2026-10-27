# Bounded native-input prefetch

The optional `--native-prefetch` sidecar mode receives native frames into two
independent pinned buffers. It overlaps preparation of the next seekdb batch
with GPU input copying and fragment execution. It keeps the same transaction
scan, supported SQL shapes, fragment limits and result checks. There is no
cross-query table cache, and no seekdb binary or Sirius-library rebuild is needed.

```sh
python tools/sirius/sidecar.py \
  --socket /absolute/private/run/sirius.sock \
  --library /absolute/path/libseekdb_sirius_bridge.so \
  --host-pin-mib 64 --native-prefetch --native-chunk-mib 128 --warmup
```

Use the same CUDA, library and configuration environment described in
[SIDECAR.md](SIDECAR.md). The seekdb server's negotiated frame bound must fit the
host buffer (`SEEKDB_SIRIUS_FRAME_MIB=64` in this experiment). With 64 MiB frames,
prefetch reserves 128 MiB total pinned input instead of 64 MiB. The flag defaults
off. If the first pin allocation fails, the existing pageable fallback remains
and readiness reports `native_prefetch=False`; failure to allocate the second
buffer aborts startup and releases the first.

## Ownership and failure handling

The receive thread exclusively owns socket reads and batch ACK writes. After a
complete frame passes its row/length bounds, it sends ACK and publishes the
owned slot. That ACK permits seekdb to reuse its sender buffer; it does not
permit the receiver slot to be overwritten or establish query success.

The GPU-side Arrow iterator borrows the slot. Only after the existing
synchronized `push_arrow` has consumed it does the iterator return the slot to
the free queue. Two slots bound receive-ahead; a full queue applies backpressure
without allocating more storage. The source creates no per-row Python objects.
Aggregate fragment and output bounds remain enforced independently.

On failure, the handler stops prefetch, interrupts a blocked receive with
read-side socket shutdown and joins the receiver before sending an error or
releasing pinned memory. Stopping also wakes a producer waiting for a free slot.
An early-ACKed producer can encounter a write error if a later GPU fragment
fails; the SQL statement still fails, and no partial result is returned.
The next connection can reuse the buffers. Startup/bind failures also release
both allocations.

## Measurement method

The exact previous-version comparison gave these hot medians (seconds):

| Query | Previous bridge | Prefetch | Latency reduction | CPU8 |
|---|---:|---:|---:|---:|
| SUM + COUNT | 1.5358 | 1.4809 | **3.58%** | 0.4145 |
| COUNT | 1.0759 | 1.0385 | **3.47%** | 0.3763 |

The two additional runs comparing the new disabled/enabled paths gave pooled
medians of 1.5379 → 1.4763 s (4.01%) and 1.0803 → 1.0427 s (3.48%). All **390
full-input calls** across the three experiments passed exact result checks,
including first calls and separate diagnostics. Each experiment contains twenty
hot samples per query/mode; the table above uses only the exact-old-source run.

This is a modest live-query improvement, not a 2× acceleration, and CPU8 remains
faster. No snapshot preparation cost is excluded: each GPU query rescans seekdb.
Diagnostic input waiting still dominates; prefetch hides some copy/execution
delay but cannot remove the serial producer. Parallel native scans and a bounded
multi-producer protocol remain the larger next step; the serial eligibility
guard has not been relaxed.

The October 27 experiment uses the existing 75-million-row SF500 customer
projection, one visible RTX 5880 Ada GPU, 64 MiB frames and a 128 MiB fragment
target. CPU8, serial GPU receive and prefetched GPU receive use the same SQL
expressions and scan hints. Every GPU call must consume all 75 million rows and
match the previously validated source oracle exactly.

[prefetch_benchmark.py](../../unittest/sql/sirius/prefetch_benchmark.py) shuffles
the three modes within each round, in one warmed GPU process. Both input buffers
are allocated before measurement so mode switching does not measure allocation.
The baseline uses only the first buffer. First calls and diagnostic calls are
excluded from hot medians. Two 20-round runs use independent seeds. An additional
20-round comparison loads the exact previous `sidecar.py` from commit `fcb4da86`
as the baseline, so the result does not depend solely on a refactored disabled
path. Its source hash is recorded.

The machine is shared; per-process GPU samples are retained. Absolute times
must not be compared directly with the previous day's experiment as evidence
of improvement or regression. The meaningful comparison is between interleaved
modes in this experiment. The optional receive thread cannot parallelize the
seekdb storage scan and does not change the CPU fallback coverage.
No positive SM samples were observed for other GPU processes during these runs;
one-second sampling cannot rule out shorter activity or CPU interference. Worker
peak RSS was about 1.44 GiB, excluding the separate seekdb server. No data file
was created or copied. Test sidecars were stopped after verification, releasing
their GPU and pinned allocations, and the original SF500 source was unchanged.

## Validation

- 31 native-input/chunk tests cover serial and prefetched merging, NULL keys and
  sums, empty inputs, frame limits, output/overflow errors, early ACK, prevention
  of premature buffer reuse, bounded receive-ahead, disconnect, cancellation
  of blocked receive/free-slot waits, retaining a failed importer batch until
  receive-ahead stops, and pin cleanup after startup failure.
- Nine existing C++ proxy transport tests passed.
- The real `--native-prefetch` sidecar passed all 16 native SQL/transaction
  checks, including own writes, commit, rollback and delete visibility.
- Cancelling an active full-size query returned SQL error 1317 in about 2 ms;
  the same SQL connection recovered, and the next complete GPU query matched
  the oracle. This is a measured example, not a cancellation-latency guarantee.

Raw evidence: [2026-10-27-prefetch](../../unittest/sql/sirius/results/2026-10-27-prefetch/).
