# Direct-load task snapshot

This directory was migrated from the preserved GitLab `obqa/direct_load`
snapshot at commit `ba498f67422a249ee4517b548e9ecadd22b68033`.

The original `load_file/` fixture directory is intentionally excluded. Its
tracked contents total about 8 GB and include 22 individual CSV files larger
than GitHub's 100 MB object limit. Supply test fixtures separately when running
the workload.

The prebuilt `locality/locality.jar` and `mytest/jar/mytest.jar` runtime
artifacts are also excluded from this source branch. Rebuild or supply those
dependencies separately before running the corresponding workloads.

Runtime artifacts from the snapshot (logs, PID files, editor swap files, and
Finder metadata) were also excluded from this branch.
