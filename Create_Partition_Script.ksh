#!/bin/ksh
. /home/tws_user/tivoli_profile.sh
dt=`date`
DBID=${1}
IND=${2}
Log_File=/data01/InfoVision/MICS_home/process/Shell/CTL/List_Det.log
Password_File=/data01/InfoVision/MICS_home/process/Shell/PARAM/PassWord.ctl
SERVERNAME=`grep ${DBID} ${Password_File} | cut -f3 -d"|" | tr '[:lower:]' '[:upper:]'| tr -d ' '`
USER_NAME=`grep ${DBID} ${Password_File} | cut -f4 -d"|" | tr '[:lower:]' '[:upper:]'| tr -d ' '`
PWD=`grep ${DBID} ${Password_File} | cut -f5 -d"|" | tr -d ' '`

sqlplus -s<<EOF  > ${Log_File} 
${USER_NAME}/${PWD}@${SERVERNAME}

set heading off;
set echo off;
Set pages 999;
set long 90000;


select 'IBCC_Date|'||to_char(trunc(sysdate-1),'yyyymmdd') from dual;

CALL PROC_CREATE_PART('${IND}');

CALL proc_purg_partitions_date();

EOF

if [ "${DBID}" = "IBCC_MICS" ]
then
 Run_Date=`grep IBCC_Date ${Log_File} | cut -f2 -d"|"`
 echo "Start_Date|${Run_Date}">/data01/InfoVision/MICS_home/process/Shell/CTL/IBCC_Run_Date.txt
 echo "End_Date|${Run_Date}">>/data01/InfoVision/MICS_home/process/Shell/CTL/IBCC_Run_Date.txt
 echo "${Run_Date}">/data01/InfoVision/MICS_home/process/Shell/CTL/IBCC_BULK_DT.txt
fi

