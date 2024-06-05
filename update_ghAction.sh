#!/bin/bash

#get todays date
DATE=$(date '+%Y-%m-%d')

URL_METADATA="https://raw.githubusercontent.com/Rubber1Duck/RD_RKI_COVID19_DATA/master/dataStore/meta/meta.json"
lastModified=$(curl -s -X GET -H "Accept: application/json" "$URL_METADATA" 2>&1 | jq -r '.version')
if [[ "$lastModified" == "" ]]; then
  DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
  echo "$DATE2 : lastModified is empty! exit now"
  exit 1
fi
if [[ "$DATE" != "$lastModified" ]]; then
  DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
  echo "$DATE2 : Updated data for $DATE in actual data does not yet exist (modified date: $lastModified)"
  exit 1
fi

# print starting message
STARTTIME=`date +%s`
DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
echo "$DATE2 : Start update with actual data (last modified: $lastModified)"

# download static 7zip
DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
echo "$DATE2 : download static 7zip"
VERSION7ZIP="2301"
./get7Zip.sh ${VERSION7ZIP}

# Print message
DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
echo "$DATE2 : extract all data"
./extract.sh

# Print message 
DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
echo "$DATE2 : executing python build_metanew.py $DATE"

python ./src/build_metanew.py $DATE


# compress json files in history
rm -f ./*.xz
for file in `find dataStore/ -name "*.json"  ! -name "meta.json" -or -name "*.feather" -type f`;
  do
    DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
    SIZE1=$(stat -c%s $file)
    echo "$DATE2 : start compressing $file ($SIZE1 bytes)"
    ../../7zzs a -txz -mmt4 -mx=9 -sdel -stl -bso0 -bsp0 "$file.xz" "$file"
    SIZE2=$(stat -c%s $file.xz)
    QUOTE=$(gawk "BEGIN {OFMT=\"%.4f\"; print $SIZE2 / $SIZE1 * 100;}")
    DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
    echo "$DATE2 : finished compressing $file. New Size: $SIZE2 = $QUOTE %"
  done
rm -rf ./7zzs

# print message update finished
DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
ENDTIME=`date +%s`
TOTALSEC=`expr $ENDTIME - $STARTTIME`
TIME=`date -d@$TOTALSEC -u +%H:%M:%S`
echo "$DATE2 : Update finished. Total execution time $TIME ."
