#!/bin/bash
father_file='./'
cd oracle
for file in ${father_file}*
do
	echo "---------------------"
        echo "测试case${file}开始："
	echo "---------------------"
        python ${file} -host 127.1 -P 25409 -u test -t tt3 -p test -directory /home/maolin.mao/direct_load_regression/load_file/comman_oracle/
done
