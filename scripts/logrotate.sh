#!/usr/bin/env bash
set +x

# full path of extra logs
extra_log_path=$1
if [[ "${extra_log_path}" == "" || ! -f ${extra_log_path} ]]; then
    exit 0
fi

logMaxSize=$((64*1024)) # 64MB
maxBackupNum=5

function duExtraLog()
{
  local extra_log=$1 # full path for the extra log

  totalLogSize=0
  totalLogSize="$(du ${extra_log} | cut -f1 | tail -1)"
  echo ${totalLogSize}
}

function rotateExtraLog() {
    local extra_log=$1 # full path for the extra log

    logSize=`duExtraLog ${extra_log}`

    if [ "${logSize}" -gt "${logMaxSize}" ];then
      currentTime="$(date "+%Y_%m_%dT%H:%M:%S")"
    	cp ${extra_log} ${extra_log}."${currentTime}"
    	cat /dev/null > ${extra_log}

    	logCount="$(ls -l ${extra_log}.* | wc -l)"
    	while [[ "${logCount}" -gt "${maxBackupNum}" ]];do
            lastFile="$(ls -rl ${extra_log}.* | tail -1 | awk '{print $9}')"
            rm -f ${lastFile}
            logCount="$(ls -l ${extra_log}.* | wc -l)"
        done
    fi
}

rotateExtraLog ${extra_log_path}

