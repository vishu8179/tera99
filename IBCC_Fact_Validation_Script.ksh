#!/bin/ksh
echo `date`> time_IBCC_Valid.txt
dt=`date`
DBID=IBCC_MICS
echo "Executing Validation Script on $dt" > IBCC_Validation.log
Password_File=/data01/InfoVision/MICS_home/process/Shell/PARAM/PassWord.ctl
SERVERNAME=`grep ${DBID} ${Password_File} | cut -f3 -d"|" | tr '[:lower:]' '[:upper:]'| tr -d ' '`
USER_NAME=`grep ${DBID} ${Password_File} | cut -f4 -d"|" | tr '[:lower:]' '[:upper:]'| tr -d ' '`
PWD=`grep ${DBID} ${Password_File} | cut -f5 -d"|" | tr -d ' '`
v_date=25-APR-2099
for line in `cat proc_list.txt`
do 
echo ${line}>>IBCC_Validation.log 
sqlplus -s<<EOF  >>IBCC_Validation.log
${USER_NAME}/${PWD}@${SERVERNAME}

set heading off;
set echo off;
Set pages 999;
set long 90000;


CALL ${line}('${v_date}');


EOF
done
sqlplus -s<<EOF  >>IBCC_Validation.log
${USER_NAME}/${PWD}@${SERVERNAME}

set heading off;
set echo off;
Set pages 999;
set long 90000;



CALL proc_MSTR_UNKN_CNT('${v_date}');

EOF
 
echo `date`>> time_IBCC_Valid.txt
