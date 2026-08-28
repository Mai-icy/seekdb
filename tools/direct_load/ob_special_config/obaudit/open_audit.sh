function print_event()
{
   msg="$1"
   echo `date  '+[%Y-%m-%d %H:%M:%S]'` $msg
}


if [ $# -lt 3 ] ; then
  print_event "Please apply svr_ip , svr_port and tenant_name."
  echo "e.g. sh open_audit.sh 127.1 2828 oracle_tt_1"
  exit 1
fi

AUDIT_DIST='OS' #'DB,EXTENDED'
svr_ip=$1
svr_port=$2
tenant_name=$3
sys_conn="obclient -h$svr_ip -P$svr_port -uroot@sys -Doceanbase -A"
audit_conn="obclient -h$svr_ip -P$svr_port -uORAAUDITOR@$tenant_name -pORAAUDITOR -A"

get_compatibility_mode()
{
  compatibility_mode=$(echo "select compatibility_mode from __all_tenant where tenant_name='$tenant_name'" | $sys_conn |grep -v compatibility_mode )
  if [ -z "$compatibility_mode" ];then
    print_event "$tenant_name isn't exist."
    exit 1
  fi
  echo $compatibility_mode
}

audit_oracle_tenant()
{
  tnt_sys_conn="obclient -h$svr_ip -P$svr_port -uSYS@$tenant_name -DSYS -A"
  echo "ALTER SYSTEM set audit_trail = '$AUDIT_DIST';" | $tnt_sys_conn -vv
  echo "alter system set audit_sys_operations=1;" | $tnt_sys_conn -vv
  echo "ALTER USER ORAAUDITOR ACCOUNT UNLOCK;" | $tnt_sys_conn -vv

  sleep 3
  echo "AUDIT all statements;" | $audit_conn -vv
  echo "alter system flush plan cache;" | $tnt_sys_conn -vv
  trail_audit_query="select * from (select /*+QUERY_TIMEOUT(100000000)*/ EXTENDED_TIMESTAMP,username ,owner, obj_name,action,sql_text from DBA_AUDIT_TRAIL where EXTENDED_TIMESTAMP>to_date('`date '+%Y-%m-%d %H:%M:%S' --date '2 second ago'`','yyyy-mm-dd hh24:mi:ss') order by 1 desc) T where rownum<10"

  sleep 4
  echo "$trail_audit_query" | $tnt_sys_conn -vv -t
  is_success=$(echo "select count(*) cnt from ($trail_audit_query) T" | $tnt_sys_conn |grep -vi cnt)
  if [ "$is_success" -gt 0 ];then
    print_event "Open audit for $tenant_name on $svr_ip:$svr_port successfully."
  else
    print_event "Failed on open audit for $tenant_name on $svr_ip:$svr_port. Please check it manually."
  fi
}

audit_mysql_tenant()
{
  tnt_sys_conn="obclient -h$svr_ip -P$svr_port -uroot@$tenant_name -Doceanbase -A"
  echo "ALTER SYSTEM set audit_trail = '$AUDIT_DIST';" | $tnt_sys_conn -vv
  echo "alter system set audit_sys_operations=1;" | $tnt_sys_conn -vv
  echo "ALTER USER ORAAUDITOR ACCOUNT UNLOCK;" | $tnt_sys_conn -vv
  echo "alter system set _audit_mode = 'oracle';" | $tnt_sys_conn -vv

  sleep 3
  echo "AUDIT all statements;" | $audit_conn -vv
  echo "alter system flush plan cache;" | $tnt_sys_conn -vv
  trail_audit_query="select /*+QUERY_TIMEOUT(100000000)*/ gmt_modified,user_name ,obj_owner_name, obj_name,action_id,sql_text from oceanBase.__all_tenant_security_audit_record where gmt_modified>'`date '+%Y-%m-%d %H:%M:%S' --date '2 second ago'`' order by 1 desc limit 10"

  sleep 4
  echo "$trail_audit_query" | $tnt_sys_conn -vv -t
  is_success=$(echo "select count(*) cnt from ($trail_audit_query) T ;" | $tnt_sys_conn |grep -vi cnt)
  if [ "$is_success" -gt 0 ];then
    print_event "Open audit for $tenant_name on $svr_ip:$svr_port successfully."
  else
    print_event "Failed on open audit for $tenant_name on $svr_ip:$svr_port. Please check it manually."
  fi

}


echo ">>>>>>>>>>>>>"
if [ "$(get_compatibility_mode)" == 1 ];then
  print_event "$tenant_name is a oracle tenant. Open it's audit now ..."
  audit_oracle_tenant
elif [ "$(get_compatibility_mode)" == 0 ];then
  print_event "$tenant_name is a mysql tenant. Open it's audit now ..."
  audit_mysql_tenant
else
  print_event "Please check input parameters: $1 $2 $3"
fi
echo 
