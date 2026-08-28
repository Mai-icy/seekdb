#open batch process multi statement optimization
alter system set ob_enable_batched_multi_statement=true tenant=all;

# das support retry for read only trx
alter system set _enable_partition_level_retry=false;

# resolve ob4038 error 
#alter system set enable_auto_refresh_location_cache=true;
#alter system set auto_refresh_location_cache_rate_limit=100;
#alter system set auto_broadcast_location_cache_rate_limit=100;

# tpch optimize
alter system set _rowsets_enabled=false tenant = all;  

# pushdown
alter system set _pushdown_storage_level = 3 tenant=all;

# enable schema recycle
alter system set schema_history_recycle_interval='10s'; 
alter system set schema_history_expire_time='2h';

# https://yuque.antfin-inc.com/ob/product_functionality_review/xsnl3w
#alter system set _follower_snapshot_read_retry_duration='5s';

# enable dml check https://yuque.antfin-inc.com/ob/product_functionality_review/zlp56c
#alter system set _enable_defensive_check=TRUE;

# enable easy keep alive                                                           
alter system set _enable_easy_keepalive=true;

# open rowsets
alter system set _rowsets_enabled = 1 tenant=all;
alter system set _rowsets_max_rows = 256 tenant=all;

# AUTO PURGE RECYCLEBIN
alter system set recyclebin_object_expire_time='4h';
alter system set _recyclebin_object_purge_frequency='10m';

# rowsets check
alter system set_tp tp_no = 367, error_code = 20, frequency = 1; 

# minor trigger major
alter system set major_compact_trigger = 3 tenant = all;

# 写入的分区校验防御，默认不打开.如果大家的测试环境对性能不是非常敏感，建议将防御检查级别调高，这样可以让防御更加严格:
alter system set _enable_defensive_check=2;

alter system set enable_async_syslog=False;

