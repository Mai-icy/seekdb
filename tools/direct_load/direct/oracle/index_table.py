#/usr/bin/evn python
#-*- encoding:utf-8 -*-
#测分区表，空表导入，随后导入多个文件，再依次（捕捉错误？不让程序断，捕捉到，打印出符合预期）导入null导入非空，数据类型不符合，列多，列少
import csv
import re
import os
import threading
import commands
import argparse
import sys


def error(result, sql):
        if ("ORA" in result[1]):
                print("ATTENTION!! excute ERROR: " + sql + result[1])
                sys.exit()
        else:
                print("excute SUCCESS: " + sql)


def success(result, sql):
        if ("ORA" not in result[1]):
                print("ATTENTION!! excute ERROR(预期外的执行成功): " + sql)
                sys.exit()
        else:
                print("excute 预期内报错: " + sql)
            

def create_table(host,port,user,tenant,password):
    create_table = "CREATE TABLE index1 ( c1  char(10) NOT NULL, c2 date NOT NULL, c3 BINARY_DOUBLE  NOT NULL, c4  date not NULL, c5 int not null, c6 number not null, c7 nvarchar2(60), c8 raw(20) not null, c9 timestamp not null, c10 varchar(10) not null, c11 varchar2(20) not null, c12 blob not null,PRIMARY KEY (c1,c2)) partition by hash(c1) partitions 500"
    index1 = "create index name on index1(c8,c3)"
    index2 = "create index index1 on index1(c5) reverse"
    index3 = " create index index2 on index1(c2) local"
    index4 = "create unique index index3 on index1(c1)"
    index5 = "create unique index index4 on index1(c1,lower(c7))"
    index6 = "create unique index index5 on index1(c1,upper(c7),c11)"
    index7 = "CREATE INDEX glo ON index1(c5,c9) global partition by range(c5) (partition p1 values less than(100),partition p2 values less than(200),partition p3 values less than(300),partition p4 values less than(400),partition p5 values less than(500),partition p6 values less than(600),partition p7 values less than(700),partition p8 values less than(800),partition p9 values less than(900),partition p10 values less than(1000),partition p11 values less than(1100),partition p12 values less than(1200),partition p13 values less than(1300),partition p14 values less than(1400),partition p15 values less than(1500),partition p16 values less than(1600),partition p17 values less than(1700),partition p18 values less than(1800),partition p19 values less than(1900),partition p20 values less than(2000),partition pmax values less than (maxvalue))"
    cmd_str = """ obclient -h%s -P%s -u%s@%s -c  -p%s -e "%s;%s;%s;%s;%s;%s;%s;%s;" """ % (host, port,user,tenant, password, create_table,index1,index2,index3,index4,index5,index6,index7)
    # print cmd_str
    result = commands.getstatusoutput(cmd_str)
    print("创表结果：")
    error(result,create_table)


def empty_load(host,port,user,tenant,password,directory):
    load_sql = "load data /*+ parallel(10) direct(true, 0) */ infile '"+directory+"/TEST.1.0.csv,"+directory+"/TEST.2.0.csv,"+directory+"/TEST.3.0.csv,"+directory+"/TEST.4.0.csv'  into TABLE index1 fields terminated by '|' enclosed by '''' lines starting by '' terminated by '\\n'"
    cmd_str = """ obclient -h%s -P%s -u%s@%s -c  -p%s -e "%s;" """ % (host, port,user,tenant, password, load_sql)
    # print cmd_str
    result = commands.getstatusoutput(cmd_str)
    print("空表导入多文件结果：")
    error(result,load_sql)


def empty_load1(host,port,user,tenant,password,directory):
    truncate = "truncate TABLE index1"
    cmd_str = """ obclient -h%s -P%s -u%s@%s -c  -p%s -e "%s;" """ % (host, port,user,tenant, password, truncate)
    # print cmd_str
    result = commands.getstatusoutput(cmd_str)
    print("truncate table：")
    error(result,truncate)
    load_sql = "load data /*+ parallel(10) direct(false, 0) */ infile '"+directory+"/TEST.1.0.csv'  into TABLE index1 fields terminated by '|' enclosed by '''' lines starting by '' terminated by '\\n'"
    cmd_str = """ obclient -h%s -P%s -u%s@%s -c  -p%s -e "%s;" """ % (host, port,user,tenant, password, load_sql)
    result = commands.getstatusoutput(cmd_str)
    print("空表导入单文件结果：")
    error(result,load_sql)


def noEmpty_load(host,port,user,tenant,password,directory):
    load_sql = "load data /*+ parallel(10) direct(true, 0) */ infile '"+directory+"/TEST.2.0.csv,"+directory+"/TEST.3.0.csv,"+directory+"/TEST.4.0.csv'  into TABLE index1 fields terminated by '|' enclosed by '''' lines starting by '' terminated by '\\n'"
    cmd_str = """ obclient -h%s -P%s -u%s@%s -c  -p%s -e "%s;" """ % (host, port,user,tenant, password, load_sql)
    result = commands.getstatusoutput(cmd_str)
    print("非空表导入多文件结果：")
    error(result,load_sql)


def Null_value(host,port,user,tenant,password,directory):
    load_sql = "load data /*+ parallel(10) direct(true, 0) */ infile '"+directory+"/null.csv' into TABLE index1 fields terminated by '|' enclosed by '''' lines starting by '' terminated by '\\n'"
    cmd_str = """ obclient -h%s -P%s -u%s@%s -c  -p%s -e "%s;" """ % (host, port,user,tenant, password, load_sql)
    result = commands.getstatusoutput(cmd_str)
    print("非空表带null值文件导入结果,预期正常导入：")
    success(result,load_sql)


def FalseType(host,port,user,tenant,password,directory):
    load_sql = "load data /*+ parallel(10) direct(true, 10) */ infile '"+directory+"/false_type.csv'  into TABLE index1 fields terminated by '|' enclosed by '''' lines starting by '' terminated by '\\n'"
    cmd_str = """ obclient -h%s -P%s -u%s@%s -c  -p%s -e "%s;" """ % (host, port,user,tenant, password, load_sql)
    result = commands.getstatusoutput(cmd_str)
    print("错误类型导入，预期被记入错误行不报错：")
    error(result,load_sql)
    load_sql = "load data /*+ parallel(10) direct(true, 0) */ infile '"+directory+"/false_type.csv'  into TABLE index1 fields terminated by '|' enclosed by '''' lines starting by '' terminated by '\\n'"
    cmd_str = """ obclient -h%s -P%s -u%s@%s -c  -p%s -e "%s;" """ % (host, port,user,tenant, password, load_sql)
    result = commands.getstatusoutput(cmd_str)
    print("错误类型导入，预期报错：")
    success(result,load_sql)


def ColumnLess_More(host,port,user,tenant,password,directory):
    load_sql = "load data /*+ parallel(10) direct(true, 0) */ infile '"+directory+"/col_less.csv'  into TABLE index1 fields terminated by '|' enclosed by '''' lines starting by '' terminated by '\\n'"
    cmd_str = """ obclient -h%s -P%s -u%s@%s -c  -p%s -e "%s;" """ % (host, port,user,tenant, password, load_sql)
    result = commands.getstatusoutput(cmd_str)
    print("少列，预期报错1525：")
    success(result,load_sql)
    load_sql = "load data /*+ parallel(10) direct(true, 0) */ infile '"+directory+"/col_more.csv'  into TABLE index1 fields terminated by '|' enclosed by '''' lines starting by '' terminated by '\\n'"
    cmd_str = """ obclient -h%s -P%s -u%s@%s -c  -p%s -e "%s;" """ % (host, port,user,tenant, password, load_sql)
    result = commands.getstatusoutput(cmd_str)
    print("多列，预期报错1525：")
    success(result,load_sql)


def load(host,port,user,tenant,password,directory):
    create_table(host,port,user,tenant,password)
    empty_load(host,port,user,tenant,password ,directory)
    empty_load1(host,port,user,tenant,password ,directory)
    noEmpty_load(host,port,user,tenant,password ,directory)
    Null_value(host,port,user,tenant,password ,directory)
    FalseType(host,port,user,tenant,password ,directory)
    ColumnLess_More(host,port,user,tenant,password ,directory)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-host", "-host", help=" hostname. eg: -t 127.1 ", type=str, dest="host", default='127.1')
    parser.add_argument("-P", "-port", help=" observer port. eg: -P 3952", type=str, dest="port", default=None)
    parser.add_argument("-u", "-user", help=" oceanbase user. eg: -u test", type=str, dest="user", default=None)
    parser.add_argument("-t", help=" tenant. eg: -t tt3", type=str, dest="tenant", default=None)
    parser.add_argument("-p", "-password", help=" tenant password. eg: -p test ", type=str, dest="password",default=None)
    parser.add_argument("-directory", "-directory", help=" directory /home/. eg: -directory /home/ ", type=str, dest="directory", default=None)
    args = parser.parse_args()
    load(args.host, args.port, args.user, args.tenant, args.password,args.directory)


if __name__ == '__main__':
    main()

