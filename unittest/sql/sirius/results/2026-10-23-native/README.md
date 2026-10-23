# Native aggregate measurements, 2026-10-23

Remote workspace: `/data/wuhao.xwh/seekdb-sirius-20261022/seekdb`.
Run artifacts: `/data/wuhao.xwh/seekdb-sirius-20261023`.
Implementation was built on `f9ca8e44` plus the native-aggregate changes committed
with these records; the embedded seekdb version therefore reports that parent.

- `native-baseline.json`: 65,536-row batches, pageable input, seven repetitions.
- `native-large-batch.json`: 262,144-row batches, pageable input, seven repetitions.
- `native-final.json`: 262,144-row batches, pinned input, warmup, fifteen repetitions.
- `native-startup.json`: twelve fresh sidecar processes, pin × warmup comparisons.
- `native-integration.json`: native SQL correctness and transaction checks.
- `native-cpp-tests.txt`: twenty C++ tests, including five native transport cases.
- `native-protocol-tests.txt`: nine Python protocol tests.
- `native-build.log`: final incremental full-seekdb build.
- `sirius.yaml`: actual single-GPU test configuration (physical GPU 1).
- `history/recommended-sf1000-2x22g-annotated.yaml`: read-only copy of historical
  dual-GPU configuration; not used by these seekdb runs.

All measured SQL results are compared with CPU output. The startup experiment
keeps seekdb storage warm, and explicitly proves the qualifying query fails when
the sidecar is absent. Final live measurements include scan and transfer costs.
The performance run precedes a final transport schema/length validation fix;
that fix was rebuilt and regression-tested and does not change valid input work.

Sirius source base: `3fcff6a0488516db09ddc7e993564eb9a98b72d3`, with the existing
Arrow overlay from the previous stage. Original library SHA256:
`cf5c490ac32f3cf7169bea8011688eedaefb634ee53741a3f475b7d729bd78ae`.
Overlay library SHA256:
`9f4163457e7f1786884bd324782a0bd9a3ee4bff49e9923635a1be94fd736744`.

See [implementation and analysis](../../../../../tools/sirius/NATIVE_AGGREGATE.md)
and [historical pin research](../../../../../tools/sirius/HOST_PIN_RESEARCH.md).
