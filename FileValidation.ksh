#!/bin/ksh

if [ $# -lt 1 ]; then

 echo "Number of Parametes are less than 1"
 exit 0

fi

SrcId=$1
CntrFleLoc=`echo "/data01/InfoVision/MICS_home/Landing"`

	echo "=========================================================="
	awk -F"|" '{print $0}' ${CntrFleLoc}/Cntrl.csv | grep -i  $SrcId | while read row
	do
		echo $row
		CircleId=`echo $row | cut -d '|' -f3 | cut -d '_' -f1`
		SrcFileId=`echo $row | cut -d '|' -f3 | cut -d '_' -f2`



		LandDir=`echo "/data01/InfoVision/MICS_home/Landing/${SrcFileId}/${SrcFileId}_${CircleId}"`
		ProcDir=`echo "/data01/InfoVision/MICS_home/Proc/${SrcFileId}/${SrcFileId}_${CircleId}"`
		ArchDir=`echo "/data01/InfoVision/MICS_home/Archive/${SrcFileId}/${SrcFileId}_${CircleId}"`


		rm -f  ${ProcDir}/${CircleId}_${SrcFileId}_Cntrl.log



		#cd /data01/InfoVision/MICS_home/Landing/FLN/FLN_45/

		ls -ltr ${LandDir} | grep -i cmp | awk -F" " '{ print $8,$9}' > ${LandDir}/File_lst.txt


		if [ -s ${LandDir}/File_lst.txt ] 
		then 

			cat ${LandDir}/File_lst.txt | while read fl 
			do 
				y=csv
				flname=`echo $fl | awk -F" " '{print $2}'| awk -F"." '{print $1".csv"}' cmp="$y" `
				timest=`echo $fl | awk -F" " '{print $1}'`
				filecnt=`wc -l $LandDir/$flname | awk -F" " '{print $1}'`


				CmpFile=`echo $flname | sed 's/csv/cmp/g'`

				#cp $LandDir/$flname $ProcDir/$flname
				#cp $LandDir/$CmpFile $ProcDir/$CmpFile

				cp $LandDir/$flname $ArchDir/$flname	
				cp $LandDir/$CmpFile $ArchDir/$CmpFile
                		cp $LandDir/$flname $ProcDir/$flname


				#sed -n '2,$p' $LandDir/"$flname" | awk -F"," '{ print $0",\042"FNAME"\042"",\042"tmst"\042"",\042"fc"\042"}' FNAME="$flname"  tmst="$timest"  fc="$filecnt" >> $ProcDir/$flname
				echo "${CircleId}|${flname}|${filecnt}|${timest}" >> ${ProcDir}/${CircleId}_${SrcFileId}_Cntrl.log

			done
		fi
done
