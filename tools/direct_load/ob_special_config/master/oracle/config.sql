#open batch process multi statement optimization
alter system set ob_enable_batched_multi_statement=1;

set global recyclebin=on;
# minor trigger major
alter system set major_compact_trigger = 3;
