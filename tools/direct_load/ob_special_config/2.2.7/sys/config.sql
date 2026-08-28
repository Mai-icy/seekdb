#open batch process multi statement optimization
alter system set ob_enable_batched_multi_statement=1 tenant=all;

# resolve ob4038 error 
alter system set enable_auto_refresh_location_cache=true;
alter system set auto_refresh_location_cache_rate_limit=100;
alter system set auto_broadcast_location_cache_rate_limit=100;

# enable schema recycle
alter system set schema_history_recycle_interval='10s'; 
alter system set schema_history_expire_time='2h';

# enable easy keep alive
alter system set _enable_easy_keepalive=true;


# auto purge recyclebin
alter system set recyclebin_object_expire_time='4h';
alter system set _recyclebin_object_purge_frequency='10m';
# 并行转储
alter system set _enable_parallel_minor_merge=true tenant=all;
# support expression parameter count > 65535,3.2.3 开始支持 10万 in表达式。默认是关闭的
alter system set _max_function_param_num =2000000;
