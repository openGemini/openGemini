#!/usr/bin/env bash
set +x

logMaxSize=$((64*1024))
maxBackupNum=5
meta_extra="meta_extra.log"
sql_extra="sql_extra.log"
storage_extra="storage_extra.log"

function duMetaExtraLog()
{
    totalLogSize=0
	totalLogSize="$(du ${meta_extra} | cut -f1 | tail -1)"
    echo ${totalLogSize}
}

function duSqlExtraLog()
{
    totalLogSize=0
	totalLogSize="$(du ${sql_extra} | cut -f1 | tail -1)"
    echo ${totalLogSize}
}

function duStoreExtraLog()
{
    totalLogSize=0
	totalLogSize="$(du ${storage_extra} | cut -f1 | tail -1)"
    echo ${totalLogSize}
}

metaLogSize=`duMetaExtraLog`
sqlLogSize=`duSqlExtraLog`
storeLogSize=`duStoreExtraLog`

if [ "${metaLogSize}" -gt "${logMaxSize}" ];then
    currentTime="$(date "+%Y_%m_%dT%H:%M:%S")"
	cp meta_extra.log meta_extra.log."${currentDate}"."${currentTime}"
	echo "" > meta_extra.log
	
	metaCount="$(ls -l meta_extra.log* | wc -l)"
	while [[ "${metaCount}" -gt "${maxBackupNum}" ]];do
        lastFile="$(ls -l meta_extra.log* | tail -1 | awk '{print $9}')"
        rm -f ${lastFile}
        metaCount="$(ls -l meta_extra.log* | wc -l)"
    done
fi

if [ "${sqlLogSize}" -gt "${logMaxSize}" ];then
    currentTime="$(date "+%Y_%m_%dT%H:%M:%S")"
	cp sql_extra.log sql_extra.log."${currentTime}"
	echo "" > sql_extra.log
	
	sqlCount="$(ls -l sql_extra.log* | wc -l)"
	while [[ "${sqlCount}" -gt "${maxBackupNum}" ]];do
        lastFile="$(ls -l sql_extra.log* | tail -1 | awk '{print $9}')"
        rm -f ${lastFile}
        sqlCount="$(ls -l sql_extra.log* | wc -l)"
    done
fi

if [ "${storeLogSize}" -gt "${logMaxSize}" ];then
    currentTime="$(date "+%Y_%m_%dT%H:%M:%S")"
	cp storage_extra.log storage_extra.log."${currentTime}"
	echo "" > storage_extra.log
	
	storeCount="$(ls -l storage_extra.log* | wc -l)"
	while [[ "${storeCount}" -gt "${maxBackupNum}" ]];do
        lastFile="$(ls -l storage_extra.log* | tail -1 | awk '{print $9}')"
        rm -f ${lastFile}
        storeCount="$(ls -l storage_extra.log* | wc -l)"
		echo "$storeCount"
    done
fi


