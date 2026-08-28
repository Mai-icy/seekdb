# 设置你的环境
work_dir="/linda"
clog_path="/data/1/linda"
data_path="/data/0/linda"
dev_name="eth0"
# 设置你的环境
#设置旁路导入难度,hard_mode=0,不开locality变更，hard_mode=1，开locality变更;restart_mode是否开启observer重启
hard_mode=1
restart_mode=1
#设置旁路导入难度


reboot_stat='FAIL'
function reset_report
{
create_tenant_mysql_stat='FAIL'
create_tenant_oracle_stat='FAIL'      
mysql_stat='FAIL'
          oracle_stat='FAIL'
     locality_stat='FAIL'
}
reset_report
function dump_report
{
   echo "=============TEST RESULT ============"
   echo "REBOOT              : $reboot_stat"
   echo "CREATE MYSQL TENANT : $create_tenant_mysql_stat"
   echo "[MYSQL TEST         : $mysql_stat"
   echo "CREATE ORACLE TENANT: $create_tenant_oracle_stat"
   echo "[ORACLE TEST        : $oracle_stat"
if [  "$hard_mode" == "1" ] ;then
   echo "LOCALITY TEST       : $locality_stat"
fi
   echo "====================================="

   return $1 
}

function update_ob
{
 cd $target_obstart/mytest
 if [ -z "$target_observer" ];then
   echo "[INFO] Download ob binary from 8877 server..."
   sh update_bin_by_ver.sh master
   mkdir -p bin && touch bin/observer
 else
   if [ ! -f "master/bin/obproxy" ];then
       echo "[INFO] Download obproxy binary from 8877 server..."   
       sh update_bin_by_ver.sh master
       mkdir -p bin && touch bin/observer
   fi
   echo "[INFO] Download observer binary from $target_observer ..."
   if [[  "$target_observer" =~  'http' ]];then
     wget $target_observer -O master/bin/observer -o /tmp/wget.log && chmod +x master/bin/observer
   else
     cp -fr $target_observer master/bin/observer
   fi
   master/bin/observer -V
   mkdir -p bin && touch bin/observer
 fi
 cd -
}

target_obstart=`pwd`
echo $target_obstart
target_observer=$1



update_ob

cd $target_obstart/mytest
echo "[INFO] Check conf/configure.ini"
h=`hostname -i`

cp -fr conf/configure.ini.tpl conf/configure.ini
sed -i "s#127.0.0.1#$h#g" conf/configure.ini
sed -i "s#YOUR_WORKDIR#$work_dir#g" conf/configure.ini
sed -i "s#YOUR_CLOG_PATH#$clog_path#g" conf/configure.ini
sed -i "s#YOUR_DATA_PATH#$data_path#g" conf/configure.ini
sed -i "s#YOUR_DEV#$dev_name#g" conf/configure.ini

. ./setenv.sh
echo "[INFO] Reboot OB ..."
rebootob > $target_obstart/reboot.log && reboot_stat='PASS'
if [ $? -ne 0 ];then
   tail -100 $target_obstart/reboot.log
   dump_report 1 || exit 1
fi

 reset_report
 cd $target_obstart/mytest
 sleep 10
 echo "[INFO] Create MySQL Tenant ..."
 create_mysql_tenant > $target_obstart/tenant_mysql.log  && create_tenant_mysql_stat="PASS"
#if [[ $create_tenant_mysql_stat == "PASS" ]] && [[ $create_tenant_oracle_stat == "PASS" ]];then
#	create_tenant_stat="PASS"
#fi

if [ $? -ne 0 ];then
   tail -100 $target_obstart/tenant_mysql.log
 #  tail -100 $target_obstart/tenant_oracle.log
 fi
if [[ $create_tenant_mysql_stat == "FAIL" ]];then
 dump_report 1 || exit 1
fi



sleep 3
function stop_locality()
{
 pkill -9 -f "java -jar locality"
}
function start_locality()
{
 sleep 300
 cd $target_obstart/locality
 java -jar locality.jar -mode locality > locality.log
}
 if [  "$hard_mode" == "1" ] ;then
   echo "[INFO] Run Locality Test"
   start_locality & 
   #switch_mysql_tenant &switch_oracle_tenant
 fi



#查询集群端口
sys_connect="obclient -h127.1 -uroot -P2828 -A -Doceanbase"
observer_port=(`$sys_connect -e "select svr_ip,sql_port from DBA_OB_SERVERS;"`)
((observer_num=(	(${#observer_port[*]}-2))/2))
#echo $observer_num
for((i=0;i<$observer_num;i++))
#for i in {1..$observer_num}
do
#echo $i
ob=$(($i * 2))
observer[$i]=${observer_port[(($ob+2))]}
port[$i]=${observer_port[(($ob+3))]}
#echo "observer_ip:${observer[$i]},observer_port:${port[$i]}"
done
#查询集群端口


#随机重启另两台observer
function stop_observer()
{
 pkill -9 -f "restart"
}

function restart_observer()
{
 cd $target_obstart  
sh restart_observer.sh ${port[0]} ${port[1]} | tee $target_obstart/restart.log 
}
 if [  "$restart_mode" == "1" ] ;then
   echo "[INFO] Run Restart observer Test"
   restart_observer &
   #switch_mysql_tenant &switch_oracle_tenant
 fi

#随机重启另两台observer



cd $target_obstart/direct/mysql
sleep 10
 stop=0
father_file='./'
 for file in ${father_file}*
do
	echo "---------------------"
        echo "测试Mysql case${file}开始："
        echo "---------------------"
   	#echo "python ${file} -host 127.1 -P ${port[2]} -u root@mysql_tenant -D test -directory $target_obstart/load_file/common/"
	python ${file} -host 127.1 -P ${port[2]} -u root@mysql_tenant -D test -directory $target_obstart/load_file/common/ | tee $target_obstart/loaddata.log

	grep "ERROR" $target_obstart/loaddata.log 2>&1 >/dev/null  && stop=1
     if [ $stop -eq 1 ];then
	echo "[INFO] load data mysql GAME OVER"
	echo "Stop Mysql load data"
	if [  "$hard_mode" == "1" ] ;then
	stop_locality > /dev/null && echo "Stop Locality Test"
     	#dump_report 1 || exit 1
	fi
	if [  "$restart_mode" == "1" ] ;then
	stop_observer > /dev/null && echo "Stop Restart Test"
	fi
	dump_report 1 || exit 1
     fi
done
   echo "[INFO] load data mysql WIN"
   mysql_stat='PASS'
#   oracle_stat='PASS'
   if [ $mysql_stat == "FAIL" ];then
     dump_report 1 || exit 1
   fi



cd $target_obstart/mytest
. ./setenv.sh
 sleep 10
echo "[INFO] Create ORACLE Tenant ..."
 create_oracle_tenant > $target_obstart/tenant_oracle.log  && create_tenant_oracle_stat="PASS"
echo $create_tenant_oracle_stat
if [ $? -ne 0 ];then
 #  tail -100 $target_obstart/tenant_mysql.log
   tail -100 $target_obstart/tenant_oracle.log
 fi
if [[ $create_tenant_oracle_stat == "FAIL" ]];then
  echo "Create ORACLE Tenant FAILED"
  stop_locality > /dev/null && echo "Stop Locality Test"
  stop_observer > /dev/null && echo "Stop Restart Test"
  dump_report 1 || exit 1
fi


cd $target_obstart/direct/oracle
sleep 10
 stop=0
father_file='./'
for file in ${father_file}*
do
        echo "---------------------"
        echo "测试ORACLE case${file}开始："
        echo "---------------------"
	python ${file} -host 127.1 -P ${port[2]} -u test -t oracle_tenant -p test -directory $target_obstart/load_file/comman_oracle/ | tee $target_obstart/loaddata.log
        grep "ERROR" $target_obstart/loaddata.log 2>&1 >/dev/null  && stop=1
     if [ $stop -eq 1 ];then
        echo "[INFO] load data oracle GAME OVER"
        echo "Stop Oracle load data"
        if [  "$hard_mode" == "1" ] ;then
        stop_locality > /dev/null && echo "Stop Locality Test"
        #dump_report 1 || exit 1
        fi
	if [  "$restart_mode" == "1" ] ;then
        stop_observer > /dev/null && echo "Stop Restart Test"
        fi
   #pkill -9 -f "change_mysql_tenant_primary_zone" > /dev/null && echo "stop obtest"
        dump_report 1 || exit 1
fi
done

   echo "[INFO] load data oracle WIN"
   oracle_stat='PASS'
#   oracle_stat='PASS'
   if [ $oracle_stat == "FAIL" ];then
  stop_locality > /dev/null && echo "Stop Locality Test"
  stop_observer > /dev/null && echo "Stop Restart Test"
     dump_report 1 || exit 1
   fi



locality_check=0
   if [  "$hard_mode" == "1" ];then
     while [ $locality_check -eq 0 ]
     do
       stop_locality > /dev/null && echo "Stop Locality Test"
       sleep 60
       echo "[INFO] Check Locality Test Status..."
       cd $target_obstart/locality
       grep "Stop change locality" locality.log && locality_check=1
       grep "Stop change locality" locality.log && locality_stat='PASS'
     done
   fi
if [  "$restart_mode" == "1" ] ;then
        stop_observer > /dev/null && echo "Stop Restart Test"
        fi

dump_report 1 || exit 1
