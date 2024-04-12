$files=Get-ChildItem -path .\dataStore\ -filter *.feather.xz -Recurse | ForEach-Object{$_.FullName}
foreach($file in $files) {
  $filePath = Split-Path $file -Parent
  $filePath = $filePath+'\'
  C:\Programme\7-Zip\7z.exe e -o"$filePath" "$file"
  Remove-Item "$file"
}
