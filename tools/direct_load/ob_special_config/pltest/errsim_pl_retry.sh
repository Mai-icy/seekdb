# Subject: To check if proceduce handle sql retry error code exactly.
#
# Clue 1: tp_no list about PL
## EN_INNER_SQL_CONN_LEAK_CHECK = 253, 
## EN_STACK_OVERFLOW_CHECK_EXPR_STACK_SIZE = 216,
#
# Clue 2: error_code list about sql retry
## 4259 OB_ERR_DISTRIBUTED_NOT_SUPPORTED, 
## 4121 OB_RPC_SEND_ERROR, 
## 8001 OB_SERVER_IS_INIT, 
## 8002 OB_SERVER_IS_STOPPING, 
## 5150 OB_TENANT_NOT_IN_SERVER, 
## 6005 OB_TRY_LOCK_ROW_CONFLICT, 
## 6235 OB_TRANS_CANNOT_SERIALIZE, 
## 6232 OB_PARTITION_IS_SPLITTING, 
## 6236 OB_TRANS_WEAK_READ_VERSION_NOT_READY, 
## 5673 OB_ERR_BUSHY_TREE_NOT_SUPPORTED, 
## 5684 OB_PX_SQL_NEED_RETRY, 
## 5833 STATIC_ENG_NOT_IMPLEMENT,
#

tp_no_list="253
216"
error_code_list="4259
4121
8001
8002
5150
6005
6235
6232
6236
5673
5684
5833"
interval_second=600

if [ -z $1 ];then
  echo "Please apply sys tenant connect parameters. Exp. sh errsim_pl_retry.sh -h127.0.0.1 -P2828 -uroot -Doceanbase -A "
  exit 1
fi

conn_sys="mysql $@"
$conn_sys -vv -t -e "select @@version_comment"
echo `date  '+[%Y-%m-%d %H:%M:%S]'` "Run on $@ .Start:"
while true
do
  for tp_no in $tp_no_list
  do
    for error_code in $error_code_list
    do
      statement="alter system set_tp tp_no = $tp_no , error_code = $error_code , frequency = 1000;" 
      #echo $statement
      $conn_sys -vv -t -e "$statement"
      sleep $interval_second
    done
  done
done




