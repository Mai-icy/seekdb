obm=$1
[ -z "$obm" ] && obm="ob32 ob31 ob22 ob400 ob401 ob402"
x=`hostname -i`
function check_zone
{
   echo "zone status:"
   mysql $default_args -e "select \"$args\",gmt_create,gmt_modified,zone,name,if(value>100000000,usec_to_time(value),value) as value,info from __all_zone order by name,zone" 
   mysql $default_args -e "select \"$args\",a.zone,sys_part,part,lead_part,lead_sys_part from  ( select zone,count(1) as sys_part from __All_root_table group by zone) a left join (select zone,count(1) as part  from __All_virtual_meta_table group by zone) b on a.zone=b.zone left join ( select zone,count(1) as lead_part from __All_virtual_meta_table where role=1 group by zone) c on c.zone=b.zone left join  ( select zone,count(1) as lead_sys_part from __All_root_table where role=1 group by zone) d on d.zone=c.zone"
   mysql $default_args -e "select \"$args\",*from __all_virtual_upgrade_inspection where info!='succeed';"
}

function check_event
{
   mysql $default_args -e "select \"$args\",gmt_create,event,value1,value2,value3,value4  from __all_rootservice_event_history  order by gmt_create desc  limit 30"
   mysql $default_args -e "select \"$args\",round((time_to_usec(gmt_modified) - time_to_usec(gmt_create))/1000000/60) as mins,sql_text,job_id,gmt_create,gmt_modified,tenant_id, job_type, job_status from oceanbase.__all_rootservice_job where job_type='ALTER_TENANT_LOCALITY' order by job_id desc"
   #mysql $default_args -e "select \"$args\",zone,count(1) from __All_virtual_meta_table where table_id = (select tablegroup_id from __All_virtual_tablegroup where tablegroup_name='tpcc_group' ) and role=1 group by 1;"
}
function check_mem
{
    echo "memory status:"
    mysql $default_args -e "select \"$args\",tenant_id,svr_ip,svr_port, mod_name,hold/1000/1000/1000 as hold_G,used/1000/1000/1000 as used_G from __all_virtual_memory_info where hold>1000000000 order by hold  desc"
    #mysql $default_args -e "select \"$args\", table_name,t.tenant_id, svr_ip,version, is_active, mem_used/1024/1024/1024 as m from  __all_virtual_memstore_info i,__all_table t  where  i.tenant_id=t.tenant_id and i.table_id = t.table_id and i.table_id in  (select distinct table_ID from __All_virtual_meta_table) order by m desc limit 10"
  mysql $default_args  -e "select svr_ip,svr_port,tenant_id,ACTIVE_SPAN/1024/1024/1024 as active_span, FREEZE_TRIGGER/1024/1024/1024 as freezed,FREEZE_CNT,MEMSTORE_USED/1024/1024/1024 as mem_used , MEMSTORE_LIMIT/1024/1024/1024 as mem_limit from  gv\$ob_memstore order by FREEZE_CNT desc limit 10"
}
function check_server
{
 echo "server status:"
 mysql $default_args -e "select * from __all_virtual_server_compaction_progress where tenant_id = 1004"
 #mysql $default_args -e "select \"$args\",a.start_service_time,a.zone,a.svr_ip,a.status,a.block_migrate_in_time as bmit,a.build_version,a.with_rootserver as is_rs,unit_num,migrating_unit_num as m_un,merged_version,part_count,sys_part_count,leader_count,((total_size-free_size)/1024/1024/1024) as Gused,Gversion from __All_server a left join __all_virtual_server_stat b on a.svr_ip=b.svr_ip and a.svr_port=b.svr_port left join __all_virtual_disk_stat c on b.svr_ip = c.svr_ip and b.svr_port=c.svr_port left join (select svr_ip,svr_port,sum(data_size)/1024/1024/1024 Gversion from __all_virtual_meta_table group by 1) d on d.svr_ip=c.svr_ip and d.svr_port=c.svr_port left join (select svr_ip,count(1) as part_count from __All_virtual_meta_table group by 1) e  on e.svr_ip = d.svr_ip left join (select svr_ip,count(1) as sys_part_count from __All_root_table group by 1) f on e.svr_ip=f.svr_ip"
}
function check_replica
{
  echo "replica status"
  mysql $default_args -e "select \"$args\",zone,svr_ip,svr_port,data_version,count(1) as c from __all_virtual_meta_table group by 1,2,3,4 order by 1,2,3,4"
  mysql $default_args -e "select \"$args\",source,is_replicate,count(1) from __all_virtual_rebalance_task_stat group by 1,2"
  mysql $default_args -e "select \"$args\",destination,is_replicate,count(1) from __all_virtual_rebalance_task_stat group by 1,2"
  echo "check clog ..."
  mysql $default_args -e "select \"$args\",parent,t.table_id, t.tenant_id,t.table_name, svr_ip,svr_port,role,last_index_log_id,last_index_log_timestamp,last_log_id,active_freeze_version from __all_virtual_clog_stat,__all_table t where  t.table_id=__all_virtual_clog_stat.table_id and  is_offline=0 and is_in_sync=0"
  mysql $default_args -e "select \"$args\",svr_ip,table_name,a.tenant_id,a.table_id,data_size/1024/1024/1024 from __All_virtual_meta_table a,__all_table b where a.table_id= b.table_id order by data_size desc limit 20;"
  mysql $default_args -e "select \"$args\",svr_ip,version, count(*), sum(macro_block_count) macro_count, sum(use_old_macro_block_count)*100/sum(macro_block_count) as reuse_pct from __all_virtual_partition_sstable_merge_info group by svr_ip,version;"
}

function quit
{
  echo "run: obclient $default_args -c -A"
  echo "python2  ./dooba -h127.0.0.1 -P2828 -uroot@sys#${args}.${USER}"
}

for args in $obm
do
 default_args="-Doceanbase -v -h$x -P2828 -uroot@sys#${args}.${USER}"
 check_mem
done

 #check_replica
for args in $obm
do
 default_args="-Doceanbase -v -h$x -P2828 -uroot@sys#${args}.${USER}"
 #check_zone
done
for args in $obm
do
 default_args="-Doceanbase -v -h$x -P2828 -uroot@sys#${args}.${USER}"
 check_server
done
for args in $obm
do
 default_args="-Doceanbase -v -h$x -P2828 -uroot@sys#${args}.${USER}"
 check_event
done

for args in $obm
do
 default_args="-Doceanbase -v -h$x -P2828 -uroot@sys#${args}.${USER}"
quit
done
