#open batch process multi statement optimization
alter system set ob_enable_batched_multi_statement=1 tenant=all;

# das support retry for read only trx
alter system set _enable_partition_level_retry=true;

# resolve ob4038 error 
alter system set enable_auto_refresh_location_cache=true;
alter system set auto_refresh_location_cache_rate_limit=100;
alter system set auto_broadcast_location_cache_rate_limit=100;

# tpch optimize
alter system set _rowsets_enabled=true tenant = all;  

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

# enable rpc compress https://yuque.antfin-inc.com/ob/product_functionality_review/enibd2
alter system set default_transport_compress_func='lz4_1.0';

alter proxyconfig set enable_index_route='true';
alter proxyconfig set enable_reroute='true';
alter proxyconfig set enable_ob_protocol_v2='true';

# AUTO PURGE RECYCLEBIN
alter system set recyclebin_object_expire_time='4h';
alter system set _recyclebin_object_purge_frequency='10m';
# 并行转储
alter system set _enable_parallel_minor_merge=true tenant=all;

# enable sql nio, 启动参数，obs重启一个生效一个，为支持send long data请求设计，所有obs重启完成之前send long data请求可能会断链接
alter system set _enable_new_sql_nio=true;

# support expression parameter count > 65535,3.2.3 开始支持 10万 in表达式。默认是关闭的
alter system set _max_function_param_num =2000000;

# enable row purge 控制是否开启purge
alter system set _enable_row_purge=True;
alter system set ob_proxy_readonly_transaction_routing_policy = false tenant = all;
# alter system set ob_proxy_readonly_transaction_routing_policy = true tenant = sys;

# 开启死锁检测功能
alter system set _lcl_op_interval='100ms';

# sys租户执行下这两个，提高内存写坏等问题复现概率：
alter system set_tp tp_no = 241, error_code = 1, frequency = 1;
# 这个会提高 OB内部2MB内存块被OS回收的概率，减小内存块复用率，但可能会存在内存操作慢导致超时的情况
alter system set memory_chunk_cache_size = '2M';
# 写入的分区校验防御，默认不打开.如果大家的测试环境对性能不是非常敏感，建议将防御检查级别调高，这样可以让防御更加严格:
alter system set _enable_defensive_check=2;
