#!/bin/ksh 
############################################################################################## 
#### FILE          : DSRunJob.ksh                                                         ####
#### DESCRIPTION   : Starts a DataStage MasterControl type job passing                    ####
####                 all runtime parameter values.                                        ####
#### USAGE         : DSRunJob.ksh <Batch> <JobName> <InvocationId>                        ####
#### AUTHOR        :                                                                      ####
#### CREATED DATE  :                                                                      ####
#### LAST MODIFIED :                                                                      #### 
##############################################################################################
####    MODIFIED DATE    #                 MODIFIED COMMENT                               ####
####    2010/12/21       # Added the functionality of Restart or Reset run Mode.          ####
####                     # For restart Create '.restart' override file.                   ####
####                     # For Reset run Create '.resetrun' override file.                ####
####    2011/01/05       # Added Check for Parameter fetched from the Common ETL DB       ####
####                     # correctly                                                      ####
##############################################################################################
. /home/tws_user/tivoli_profile.sh
PROG=`basename ${0}`
EXIT_STATUS=0


if [ ${#} -ne 3 ]; then
   echo `date '+%Y%m%d %H:%M:%S'` ${PROG} "-> Invalid parameter list."
   echo `date '+%Y%m%d %H:%M:%S'` ${PROG} "-> The script needs 4 parameters:"
   echo `date '+%Y%m%d %H:%M:%S'` ${PROG} "->     Batch"
   echo `date '+%Y%m%d %H:%M:%S'` ${PROG} "->     JobName"
   echo `date '+%Y%m%d %H:%M:%S'` ${PROG} "->     InvocationId"
   echo `date '+%Y%m%d %H:%M:%S'` ${PROG} "-> ${PROG} <Batch> <JobName> <InvocationId>"
   exit 99
fi
cr=`echo ${3} | cut -c 1-2`

if [ $3 = '99MART1' ] || [ $3 = '99MART2' ] || [ $3 = '99MART3' ] ; then
ProjectEnv=/data01/InfoVision/MICS_home/process/Shell/PARAM/${cr}_param/edwenv.${3}.ctl
else
ProjectEnv=/data01/InfoVision/MICS_home/process/Shell/PARAM/${cr}_param/bor_mics_proj.ctl
fi

. ${ProjectEnv}

Batch="${1}"
JobName="${2}"
InvocationId="${3}"
LogDate=`date '+%Y%m%d%H%M%S'`
LogFile=${LogFileDirectory}/${Batch}/${PROG}.${JobName}.${InvocationId}.${LogDate}.log
LogFileTmp=${RestartFileDirectory}/${JobName}.${InvocationId}.log
ParFile=${RestartFileDirectory}/${JobName}.${InvocationId}.par
ResetFile=${RestartFileDirectory}/${JobName}.${InvocationId}.resetrun
RestartFile=${RestartFileDirectory}/${JobName}.${InvocationId}.restart
AbendFile=${RestartFileDirectory}/${JobName}.${InvocationId}.err

export Batch  JobName  InvocationId  


### Function for running SQL ###
DBID=${DSCommonETL}
DBName=`grep $DBID $PasswordFile | awk -F"|" '{print $3}' | sed 's/^ *//g;s/ *$//g'`
UID=`grep $DBID $PasswordFile | awk -F"|" '{print $4}' | sed 's/^ *//g;s/ *$//g'`
PSWD=`grep $DBID $PasswordFile | awk -F"|" '{print $5}' | sed 's/^ *//g;s/ *$//g'`
SCN=`grep $DBID $PasswordFile | awk -F"|" '{print $6 }' | sed 's/^ *//g;s/ *$//g'`

exsql ()  {
 awk '{ if(NR == 1) {
        STR=sprintf("%s/%s@%s",UI,PW,DB)
        print "CONNECT " STR
        print "set pagesize 0;"
        print "set linesize 2000;"
        print "set MAXDATA 60000;"
        print "set heading off;"
        print "set trimspool off;"
        print "set echo off;"
        }
        print $0}' UI=$UID PW=$PSWD DB=$DBName | sqlplus -s /NOLOG | \
        sed '
            /^$/d
            /^Connected.$/d
            s/ *|/|/g
            s/| */|/g
            s/^ *//g
            s/ *$//g
            /rows selected/d
            /Session altered/d
        '
}

### Function to log the process status ###
function do_log
{ 
  echo `date '+%Y%m%d %H:%M:%S'` ${PROG} "->" $1 | tee -ai ${LogFile}
} 

### Execution begins...
touch ${LogFile}
echo "${PROG} Start Time" `date +"%r %A %d %B %Y (Julian Date: %j)"` | tee -ai ${LogFile}

## Checking for Restart
if test -f "${AbendFile}" ; then
echo "Warning : ${JobName}.${InvocationId} Re-starting..." | tee -ai ${LogFile}
StatusPreRun=RESTART
else
StatusPreRun=START
fi

## Checking for Reset Mode Run

if test -f "${ParFile}" ; then
   if test -f "${RestartFile}" ; then 
      echo "Warning : ${JobName}.${InvocationId} Run Mode for current run is RESTART..." | tee -ai ${LogFile}
      echo "Removing ${RestartFile} File..." | tee -ai ${LogFile}
      rm -rf ${RestartFile}
      mode='' 
   else 
  	if test -f "${ResetFile}" ; then
       	   echo "Warning : ${JobName}.${InvocationId} Run Mode for current run is RESET..." | tee -ai ${LogFile}
           mode=`echo -mode RESET`
           do_log " Resetting the failed job..."
           eval ${BinFileDirectory}/dsjob -run ${mode} -wait -paramfile ${ParFile} ${DSPROJECT} ${JobName}.${InvocationId}  >> ${LogFileTmp} 2>&1 
           echo "Removing ${ResetFile} File..." | tee -ai ${LogFile}
           rm -rf ${ResetFile}
        else
        	do_log " This Batch process is already running or previous .par file is not cleaned "
                do_log " For Restart or Reset run please create the override files... "
                do_log "ParFile: ${ParFile}"
         	do_log " Aborting this run ..."
	        exit 99
        fi
   fi
else
mode=''
fi

do_log " Initialing The Process..." 
do_log " Creating Dynamic Parameter File..."

echo "select PARAMETER_NAME || '=' || PARAMETER_VALUE from ${SchemaCommon}.ds_parameter where Job_Name='${JobName}' and INSTANCE_ID='${InvocationId}' ;" | exsql > ${ParFile}

### Check For valid entries in CommonETL ###
ParamExit=`cat ${ParFile} | wc -l | sed 's/^ *//g;s/ *$//g'`
if [ "${ParamExit}" = 0 ] ; then 
do_log " Not able to get the Job Pramater from the ETL-DB..."
do_log " Aborting the Job Run..." 
exit 99 
fi

do_log " Merging Parameter File with Common Environment Parameter..."
echo PasswordFile="${PasswordFile}" | sed 's/^ *//g;s/ *$//g' >> ${ParFile}
echo BinFileDirectory="${BinFileDirectory}" | sed 's/^ *//g;s/ *$//g' >> ${ParFile}
echo ParmFileDirectory="${ParmFileDirectory}" | sed 's/^ *//g;s/ *$//g' >> ${ParFile}
echo LogFileDirectory="${LogFileDirectory}" | sed 's/^ *//g;s/ *$//g' >> ${ParFile}
echo RestartFileDirectory="${RestartFileDirectory}" | sed 's/^ *//g;s/ *$//g' >> ${ParFile}
echo DatasetFileDirectory="${DatasetFileDirectory}" | sed 's/^ *//g;s/ *$//g' >> ${ParFile}
echo SeqFileDirectory="${SeqFileDirectory}" | sed 's/^ *//g;s/ *$//g' >> ${ParFile}
echo LoadFileDirectory="${LoadFileDirectory}"  | sed 's/^ *//g;s/ *$//g' >> ${ParFile}
echo RejectFileDirectory="${RejectFileDirectory}" | sed 's/^ *//g;s/ *$//g' >> ${ParFile}
echo LandingFileDirectory="${LandingFileDirectory}" | sed 's/^ *//g;s/ *$//g' >> ${ParFile}
echo ControlShellDirectory="${ControlShellDirectory}" | sed 's/^ *//g;s/ *$//g' >> ${ParFile} 
echo ArchiveFileDirectory="${ArchiveFileDirectory}" | sed 's/^ *//g;s/ *$//g' >> ${ParFile}

#grep '##DSPARAM' ${ProjectEnv} | grep -v DSPROJECT | awk -F";" '{print $1}' | sed 's/^ *//g;s/ *$//g' >> ${ParFile}

do_log " Adding Error File Name..."
echo ErrorFile="${AbendFile}" >> ${ParFile} 
do_log " Adding Job Run Status..."
echo JobStatus="${StatusPreRun}" >> ${ParFile} 

do_log " Using Below ParameterSet for Job Execution :"
for param in `cat ${ParFile}`
do
do_log "     $param "
done 

####### 
####### DataStage EXECUTION #################################################### 
####### 

do_log " Executing DataStage dsjob program..." 
do_log " eval ${BinFileDirectory}/dsjob -run -wait -paramfile ${ParFile} ${DSPROJECT} ${JobName}.${InvocationId}  > ${LogFileTmp} 2>&1 "

echo eval ${BinFileDirectory}/dsjob -run -wait -paramfile ${ParFile} ${DSPROJECT} ${JobName}.${InvocationId}  > ${LogFileTmp} 2>&1

eval ${BinFileDirectory}/dsjob -run -wait -paramfile ${ParFile} ${DSPROJECT} ${JobName}.${InvocationId}  >> ${LogFileTmp} 2>&1

jobwaiting=`grep "Waiting for job..." ${LogFileTmp}` 
if [ "${jobwaiting}" != "Waiting for job..." ]; then 
   do_log " DataStage failed to start the job" 
   failedstart=1 
else  
   do_log " DataStage successfully started the job" 
   failedstart=0 
fi
do_log " Retrieving job information..." 

${BinFileDirectory}/dsjob -jobinfo ${DSPROJECT} ${JobName}.${InvocationId} >> ${LogFileTmp} 2>&1

####### 
####### CHECK STATUS ########################################################### 
####### 
ERROR=`grep "Job Status" ${LogFileTmp}` 
ERROR=${ERROR##*\(} 
ERROR=${ERROR%%\)*} 

if [ "${failedstart}" != 0 ]; then  
   do_log " The job failed to start" 
   AuditStatus="FAILURE" 
   ## Comments="MasterControl aborted" 
   EXIT_STATUS=1 
else 
   if [ "${ERROR}" = 1 -o "${ERROR}" = 2 ]; then  
      do_log  " The job completed successfully"
      AuditStatus="SUCCESS" 
      ## Comments="" 
      EXIT_STATUS=0 
   else 
      do_log " The job aborted" 
      AuditStatus="FAILURE" 
      ## Comments="MasterControl aborted" 
      EXIT_STATUS=1 
   fi 
fi 

####### 
####### EXIT ################################################################### 
####### 

if [ "${EXIT_STATUS}" != 0 ]; then
do_log " Retaining Temporary File..."
do_log " Log File :  ${LogFileTmp}"
do_log " Par File :  ${ParFile}"
do_log " Err File :  ${AbendFile}"
else
do_log " CleaningUp Temporary File... "
if [ "${StatusPreRun}" = "RESTART" ]; then
do_log " Command : rm ${AbendFile} "
rm ${AbendFile}
fi
do_log " Command : rm ${LogFileTmp} "
rm ${LogFileTmp} 
do_log " Command : rm ${ParFile} "
rm ${ParFile}
do_log " :::::::::    Printing Job Stats    :::::::::"
${BinFileDirectory}/dsjob -report ${DSPROJECT} ${JobName}.${InvocationId} | tee -ai ${LogFile}
echo | tee -ai ${LogFile}
echo '**************************************************'  | tee -ai ${LogFile}
echo | tee -ai ${LogFile} 
fi 

do_log " Complete, exiting with status [${EXIT_STATUS}]"
echo "${PROG} End Time" `date +"%r %A %d %B %Y (Julian Date: %j)"` | tee -ai ${LogFile}
#####if [ "${EXIT_STATUS}" != 0 ]; then

####mail_file=`cat ${ControlShellDirectory}/mail_file.txt`
###dt=`date`
#####mailx -r "MICS project" -s " JOb failed for ${DSPROJECT} ${JobName}.${InvocationId} on ${dt}" ${mail_file} < ${LogFile}
###echo "Mail sent for failure"

#########fi
exit ${EXIT_STATUS}
