
#open batch process multi statement optimization
alter system set ob_enable_batched_multi_statement=1;

alter system set writing_throttling_trigger_percentage=60;
set global ob_trx_idle_timeout=1200000000;
set global ob_query_timeout= 1000000000000;
set global ob_trx_timeout= 1000000000000;
set global recyclebin='ON';
set global undo_retention=3600;
