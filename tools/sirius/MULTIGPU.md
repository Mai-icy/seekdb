# Dual GPU input placement and bounded larger fragments

The Arrow overlay now distributes successive input batches across the GPU
memory spaces configured in Sirius. Sirius schedules the resulting batches and
merges aggregate results across devices. One configured GPU retains the existing
single-device behavior. Physical devices are selected with
`CUDA_VISIBLE_DEVICES`; the overlay uses their configured logical device IDs.

Input placement is round-robin within each fragment. Each host-to-device copy
still completes before `push_arrow` returns, so native receive buffers can be
reused safely. `cudaHostAllocPortable` makes the pinned receive allocation usable
across CUDA contexts. This change does not make the seekdb storage scan parallel,
overlap H2D copies across devices, or introduce a persistent GPU table cache.

The two RTX 5880 Ada devices in the experiment each expose 49140 MiB. Their
interconnect is PCIe (`NODE` in `nvidia-smi topo -m`), with no NVLink. Other users'
GPU allocations remained in place throughout the experiments.

## SF500 results (2026-10-27)

Hot query medians, **milliseconds**, excluding pin/setup. Each entry combines
twenty samples from the forward/reverse sweeps:

| Configuration | Fragment target | Peak framebuffer per device | Resident SUM+COUNT | Resident COUNT |
|---|---:|---:|---:|---:|
| One GPU, pool fraction 0.15 | 128 MiB | 7644 MiB | 94.01 | 49.48 |
| One GPU, pool fraction 0.30 | 128 MiB | 14908 MiB | 93.61 | 49.88 |
| Two GPUs, pool fraction 0.15 each | 128 MiB | 7644 / 7640 MiB | 92.81 | 49.16 |
| One GPU, pool fraction 0.075 | 1536 MiB | 3996 MiB | 68.84 | 38.07 |
| Two GPUs, pool fraction 0.075 each | 1536 MiB | 3998 / 3992 MiB | **63.41** | **35.07** |

The combined change reduces resident latency by **32.55% / 29.12%** relative to
the first row. At the same larger fragment target and per-device pool fraction,
two GPUs give approximately **1.086×** the speed of one GPU, not 2×. Increasing
the pool alone did not give a useful improvement. Halving each device's pool
fraction from 0.15 to 0.075 retained the larger-fragment performance: observed
framebuffer use fell from about 7.46 to 3.90 GiB per card, including runtime.
The selected dual-GPU process uses about **7.80 GiB total**, slightly more than
the original single-GPU process; it does not combine the cards into one memory
space. This smaller budget is validated for this projection/aggregation workload.

Live query medians remain **1.0201 s / 0.7818 s**, compared with **1.0097 s /
0.7853 s** for the original single-GPU/small-fragment configuration. There is no
clear live-query improvement. The selected configuration's interleaved CPU8
medians are **0.3731 s / 0.3372 s**, still faster than live GPU execution.
Prepared resident queries are 5.88× / 9.62× faster than that CPU8 baseline, under
the explicitly prepared-snapshot timing contract below.

One separate dual-GPU diagnostic measured SUM+COUNT at 950 ms waiting for live
input, 52 ms copying and 12 ms executing. Resident replay measured 49 ms copying
and 11 ms executing. COUNT showed the same pattern. These are host wall-clock
diagnostics, not CUDA event durations. Parallel seekdb input and overlapping H2D
transfers, or a correctly invalidated device-resident input cache, remain the
larger opportunities. This change implements none of those yet.

All **1440 complete-input calls** in the twenty main matrix processes passed
exact result checks. The 41 unit/GPU tests and nine proxy transport tests passed.
The actual CLI sidecar also passed 18 live Arrow/snapshot checks, 16 native
SQL/transaction checks and full-query cancellation/recovery (SQL 1317, about
0.95 ms to return cancellation, followed by a correct full GPU query).
Both devices have positive per-process SM samples. Other GPU processes showed
no positive one-second SM samples, which cannot rule out shorter activity or
CPU interference.
The actual CLI execution's Sirius telemetry also records **11 HASH_GROUP_BY
tasks on GPU 0 and 10 on GPU 1**, processing about 613 MB / 606 MB respectively.
[Operator evidence](../../unittest/sql/sirius/results/2026-10-27-multigpu/operator-gpu-evidence.json)
maps computing events to each GPU executor thread, confirming aggregation ran
on both devices in addition to input placement and allocation.

Raw configurations, per-call timings, device placement, GPU samples, test logs
and cleanup evidence are in
[2026-10-27-multigpu](../../unittest/sql/sirius/results/2026-10-27-multigpu/),
with pooled results in
[summary.json](../../unittest/sql/sirius/results/2026-10-27-multigpu/summary.json).
The SF500 source and borrowed Sirius checkout/library remained unchanged. No
database was copied; the new owned library/build/log directory is about 180 MiB.
All test sidecars stopped and released GPU/pinned resources; about 546 GiB of
disk remained available.
The tested configuration can be started with
`/data/wuhao.xwh/seekdb-sirius-multigpu-20261027/start-sidecar.sh`.

## Fragment and memory controls

The default decoded-input budget remains **512 MiB per fragment**, with an
unchanged **128 MiB per batch** bound. An explicit
`SEEKDB_SIRIUS_FRAGMENT_LIMIT_MIB=2048` increases only the streamed bridge/native
fragment budget to 2 GiB. Invalid values fail instead of silently removing the
bound. The general Arrow IPC socket request still retains its existing bounds.

`--native-chunk-mib` now also accepts 384, 1024 and 1536. Targets above 384 require
the 2 GiB opt-in. The target leaves at least one maximum-sized batch of headroom,
since a fragment finishes after the batch that crosses the target. The 1536 MiB
target fits this experiment's complete 75-million-row two-column input into one
fragment, reducing repeated plan construction and partial-result merging.

The fragment budget limits decoded input, not total GPU working memory. Sirius
still needs space for intermediate tables, hash aggregation and CUDA runtime
allocations. Size `memory.gpu.usage_limit_fraction` against physical VRAM and
other tenants' current allocations. Increasing this fraction alone does not
remove scan or PCIe copy costs.

Build both the owned Arrow overlay and the bridge for these changes; the seekdb
server binary does not need rebuilding. The overlay build never modifies the
borrowed Sirius source/build tree. Run with the new library directory in
`LD_LIBRARY_PATH`, and a matching topology in `SEEKDB_SIRIUS_CONFIG`:

```sh
export CUDA_VISIBLE_DEVICES=0,1
export SEEKDB_SIRIUS_CONFIG=/absolute/path/dual-gpu.yaml
export SEEKDB_SIRIUS_FRAGMENT_LIMIT_MIB=2048
python tools/sirius/sidecar.py \
  --socket /absolute/private/run/sirius.sock \
  --library /absolute/path/new/libseekdb_sirius_bridge.so \
  --host-pin-mib 64 --native-prefetch --native-chunk-mib 1536 --warmup
```

## Measurement contract

The benchmark uses the existing SF500 customer projection: 75 million rows,
BIGINT group key, DOUBLE integer cents, and 25 groups. Every timed GPU query must
consume all rows and match the previously validated source oracle exactly.

Primary timings exclude pin allocation, snapshot capture, startup and warmup.
Live timings include the full seekdb scan, packing, transfer, H2D copy, GPU work
and TCP SQL result. Resident timings use an immutable **pinned host** snapshot
through the same TCP SQL function path; every call copies input to the GPUs and
aggregates it again. It is a benchmark snapshot without production cache
invalidation. It is not a transparent acceleration of ordinary live SELECTs.

[multigpu_benchmark.py](../../unittest/sql/sirius/multigpu_benchmark.py) interleaves
CPU8/live/resident modes within each process. Different GPU configurations need
separate processes; the matrix runs a forward and reverse sweep with ten hot
samples per case/mode/sweep. First and diagnostic calls are separate. This shared
machine can still have CPU/OS interference, so small differences across process
configurations should not be read as precise causal speedups.

The tests cover repeated pinned-slot reuse across GPU destinations, overlapping
group keys, NULL keys and all-NULL SUMs, empty batches, budget rejection and
recovery, live snapshot/transaction behavior and cancellation of a full-size SQL
query. Diagnostics record placement per device; per-process GPU samples provide
independent evidence of both devices' activity.
