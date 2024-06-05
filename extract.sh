for i in `find dataStore/ -name "*.feather.xz" -type f`;
 do
  ./7zzs e -o`dirname "$i"` "$i";
 done
