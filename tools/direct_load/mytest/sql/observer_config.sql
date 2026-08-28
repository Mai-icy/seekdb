alter system set datafile_size='270g';

#alter system set enable_syslog_recycle=true;
#alter system set  max_syslog_file_count=300;

## max_cpu = min_cpu
alter resource unit sys_unit_config min_cpu=1,max_cpu=1;
#alter resource unit sys_unit_config max_cpu=2.5;
#alter system set minor_freeze_times=10;

## bmsql
#alter system set _max_trx_size='100G';
#alter system set enable_merge_by_turn=false;

## quick recover 
#alter system set election_blacklist_interval='30s';

## 500 tenant
alter system set system_memory='6g';

## frequently dump
#alter system set freeze_trigger_percentage=20;

## easy debug
alter system set syslog_io_bandwidth_limit='3000m';

## reduce cpu%
alter system set weak_read_version_refresh_interval='1s';

## shutdown storage compress
#alter system set default_compress_func = 'NONE';

## clog recycle 
#alter system set clog_disk_utilization_threshold=90;

## enable transport compress
#alter system set clog_transport_compress_all=true;


#set global ob_query_timeout= 1000000000;
#set global ob_trx_timeout= 100000000000;
#
## 15g
#alter  resource unit sys_unit_config min_memory=16106127360,max_memory=16106127360;
#
#alter system set enable_sql_audit=false;
#alter system set cpu_quota_concurrency=2;
#alter system set server_data_copy_out_concurrency=1000;
#alter system set server_data_copy_in_concurrency=1000;
#alter system set memory_chunk_cache_size ='16G';
#alter system set minor_freeze_times=3;
#alter system set clog_transport_compress_all=false;
#alter system set trx_try_wait_lock_timeout='0ms';
#alter system set trace_log_slow_query_watermark='500ms';
#alter system set syslog_io_bandwidth_limit='3000m';
#alter system set enable_async_syslog=true;
#alter system set merger_warm_up_duration_time='0';
#alter system set merger_switch_leader_duration_time='0';
#alter system set memstore_limit_percentage=50;
#alter system set large_query_worker_percentage=10;
#alter system set minor_compact_trigger = 2;
#alter system set builtin_db_data_verify_cycle = 0;
#alter system set micro_block_merge_verify_level=0;
#alter system set system_memory='5g';
#alter system set enable_auto_leader_switch=true;                                                                                                                  
#alter system set election_blacklist_interval='30s';
#alter system set freeze_trigger_percentage=30;
#alter system set sys_bkgd_io_low_percentage=70;
#alter system set _mini_merge_concurrency = 5;
#alter system set enable_merge_by_turn=false;
#alter system set large_query_threshold='1000s';
#alter system set weak_read_version_refresh_interval=0;
#alter system set _ob_enable_prepared_statement = true;
#alter system set _clog_aggregation_buffer_amount=0 tenant=all;
#alter system set enable_early_lock_release=false tenant=all;
alter system set ignore_replay_checksum_error=True;

