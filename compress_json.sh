for i in `find dataStore/ -name "*.json"  ! -name "meta.json" -type f`;
 do
  rm "$i.xz"
  ./7zzs a -txz -mmt4 -mx=9 -sdel -stl "$i.xz" "$i";
 done
