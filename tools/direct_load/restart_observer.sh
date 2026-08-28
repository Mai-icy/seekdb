#!/bin/bash
#get user name
user=`whoami`
port[0]=$1
port[1]=$2
echo ${port[0]} ${port[1]}
x=1
for i in {1..10}
do
#随机挑一个observer
((ran=(($RANDOM % 2))))
echo $ran
port=${port[$ran]}
	touch process.txt
        ps aux | grep observer |grep "\-p $port"| grep -v grep | grep $user  > process.txt
        observer_process=`cat process.txt | awk -F" " '{ for(i=1; i<=10; i++){ $i="" }; print $0 }' `
        process_id=`cat process.txt | awk -F" " '{ print $2 }'`
        work_dir=`cat process.txt | awk -F" " '{ print $11 }' | cut -d"/" -f 1-9`
        work_dir=${work_dir%bin*}
	#echo "工作路径：$work_dir"
	echo $process_id | xargs kill -9
        #start observer
        sleep 20
        cd $work_dir;$observer_process
        mv process.txt process$x.txt
        ((x=(($x+1))))
	sleep 300
done

