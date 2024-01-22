#!/usr/bin/env bash

CURRENT_HOME=$(cd $(dirname $0); pwd)
source $(dirname "${CURRENT_HOME}")/main/init.sh

switch_on=$(grep "fstab_uuid_format_enable" $VARS_PATH/main.yml | awk '{print $2}')
format_dir=$(grep "fstab_uuid_format_dir" $VARS_PATH/main.yml | awk '{print $2}')
if [[ "true" != "$switch_on" ]]; then
    exit 0
fi

if [[ "" == "${format_dir}" ]]; then
    exit 0
fi

echo "begin format fstab"

FSTAB_FILE=/etc/fstab
FSTAB_BK=/tmp/fstab_backup

rm $FSTAB_BK
cp $FSTAB_FILE $FSTAB_BK

list=$(cat $FSTAB_FILE |grep "$format_dir"|awk '{print $1}')
echo "fstab: $list"

for element in ${list[@]}
do
  valid=$(echo "$element"|grep ^/dev)
  if [[ $valid == "" ]]; then
    continue
  fi

  disk_uuid=$(blkid|grep "$element"|awk -F ' |"' '{print $3}')
  if [[ "" == "${disk_uuid}" ]]; then
      continue
  fi

  echo "replace $element with $disk_uuid"
  sed -i "s#$element#UUID=$disk_uuid#" $FSTAB_FILE
done

mount -a
command_status=$?
if [ "${command_status}" -ne 0 ]; then
  echo "failed to format fstab, rollback it"
  cp -f ${FSTAB_BK} ${FSTAB_FILE}
  exit 1
fi

echo "format success"
