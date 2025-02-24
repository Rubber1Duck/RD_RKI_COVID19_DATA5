#!/bin/bash

#get todays date
DATE=$(date -d $1 '+%Y-%m-%d')

if [[ "$DATE" == "" ]]; then
  DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
  echo "$DATE2 : DATE is empty! exit now"
  exit 1
fi

# print starting message
STARTTIME=`date +%s`
DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
echo "$DATE2 : Start update with date: $DATE)"

# download static 7zip
DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
echo "$DATE2 : download static 7zip"
VERSION7ZIP="2409"
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
    echo -n "$DATE2 : compressing $file ($SIZE1 bytes); "
    ./7zzs a -txz -mmt4 -mx=9 -sdel -stl -bso0 -bsp0 "./$file.xz" "./$file"
    SIZE2=$(stat -c%s $file.xz)
    QUOTE=$(gawk "BEGIN {OFMT=\"%.4f\"; print $SIZE2 / $SIZE1 * 100;}")
    echo "New Size: $SIZE2 = $QUOTE %"
  done
rm -rf ./7zzs

# print message update finished
DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
ENDTIME=`date +%s`
TOTALSEC=`expr $ENDTIME - $STARTTIME`
TIME=`date -d@$TOTALSEC -u +%H:%M:%S`
echo "$DATE2 : Update finished. Total execution time $TIME ."
git add ':/dataStore/*.json'
git add ':/dataStore/*.xz'
git status -s
git commit -m "update on $1"
git push
