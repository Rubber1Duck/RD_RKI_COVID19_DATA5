$files=Get-ChildItem -path .\dataStore\ -Include *.json -Exclude meta.json -Recurse | ForEach-Object{$_.FullName}
foreach($file in $files) {
  Remove-Item "$file.xz"
  C:\Programme\7-Zip\7z.exe a -txz -mmt4 -mx=9 -sdel -stl "$file.xz" "$file"
}
