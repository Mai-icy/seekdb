# TPC-H query templates

Exported from DuckDB 1.5.6's `tpch_queries()` table function on 2026-10-23.
These are the standard fixed-parameter 22-query templates, including a CTE
form of Q15. The test runner scales Q11's 0.0001 threshold by the supplied
scale factor, identically for seekdb and the DuckDB oracle. Other templates
are unchanged except for seekdb SELECT parallelism hints.

This is a correctness/performance experiment, not an audited TPC-H result.
