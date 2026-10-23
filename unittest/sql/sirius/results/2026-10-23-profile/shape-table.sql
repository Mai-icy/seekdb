-- Executed only in the isolated port-2886 test instance. Existing tables are
-- never dropped or overwritten. Requires the previously populated 5M-row source.
SET ob_query_timeout = 600000000;
SET ob_trx_timeout = 600000000;
CREATE TABLE sirius_shapes_20261023 (
  id BIGINT PRIMARY KEY, g10 BIGINT, g1000 BIGINT, g10000 BIGINT,
  a DOUBLE, b DOUBLE, c DOUBLE, d DOUBLE
);
INSERT INTO sirius_shapes_20261023
SELECT id, MOD(id,10), MOD(id,1000), MOD(id,10000),
       amount, amount*0.5, amount*2, amount*4
FROM sirius_benchmark;
ALTER SYSTEM MAJOR FREEZE;
-- Wait for the new FROZEN_SCN to finish with STATUS=IDLE, IS_ERROR=NO.
SELECT * FROM oceanbase.DBA_OB_MAJOR_COMPACTION;
