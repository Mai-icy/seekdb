  #open batch process multi statement optimization
  alter system set ob_enable_batched_multi_statement=1;

  set global ob_query_timeout= 100000000000;
  set global ob_trx_timeout= 100000000000;
  set global ob_enable_transmission_checksum=OFF;

  set global recyclebin=on;
