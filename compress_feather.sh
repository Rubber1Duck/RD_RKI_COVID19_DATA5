for i in `find dataStore/ -name "*.feather" -type f`;
 do
  ./7zzs a -txz -mmt4 -mx=9 -sdel -stl "$i.xz" "$i";
 done
