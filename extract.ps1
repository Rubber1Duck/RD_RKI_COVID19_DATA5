$files=Get-ChildItem -path .\dataStore\ -filter *.json.xz -Recurse | ForEach-Object{$_.FullName}
foreach($file in $files) {
  $filePath = Split-Path $file -Parent
  $filePath = $filePath+'\'
  C:\Programme\7-Zip\7z.exe e -bso0 -bsp0 -o"$filePath" "$file"
}
