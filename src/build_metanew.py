import os, json, sys
import datetime as dt
import pandas as pd
import platform
from update_changes_history import update
from fallzahlen_update import f_update

def build_meta(datum):
  filename = "RKI_COVID19_XXXX-XX-XX.csv.xz"
  filename = filename.replace("XXXX-XX-XX", datum)
  if platform.system() == "Linux":
    source_path = "/home/peter/RKIData/" + filename
    dest_path = "/home/peter/RD_RKI_COVID19_DATA5/data/" + filename
  elif platform.system() == "Windows":
    source_path = "F:\\RKIData\\" + filename
    dest_path = "F:\\RD_RKI_COVID19_DATA5\\data\\" + filename
  else:
    print("Unbekannte Platform")
    exit(1)
  datetime = dt.datetime.strptime(datum, "%Y-%m-%d")
  unix_timestamp = int(dt.datetime.timestamp(datetime)*1000)
  os.rename(source_path, dest_path)

  new_meta = {
    "publication_date": datum,
    "version": datum,
    "size": os.path.getsize(dest_path),
    "filename": filename,
    "url": dest_path,
    "modified": unix_timestamp}
  
  return new_meta

if len(sys.argv) == 1:
  # for debug put startdate here
  startDatum = "2020-04-09"
else:
  # for debug put enddate here
  startDatum = sys.argv[1]
startObject = dt.datetime.strptime(startDatum, '%Y-%m-%d')
if len(sys.argv) == 1:
  endDatum = "2020-04-09"
else:
  endDatum = sys.argv[2]
endObject = dt.datetime.strptime(endDatum, '%Y-%m-%d')
print("running from", startObject, "to", endObject)
for datumloop in pd.date_range(start=startObject, end=endObject).tolist():
  startTime = dt.datetime.now()
  datum = datumloop.strftime('%Y-%m-%d')
  aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
  print (aktuelleZeit, ": running on", datum)
  new_meta = build_meta(datum)
  metaNew_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "dataStore", "meta", "meta_new.json")
  with open(metaNew_path, "w", encoding="utf8") as json_file:
    json.dump(new_meta, json_file, ensure_ascii=False)
  versionsplit = datum.split("-")
  datumversion = versionsplit[0] + versionsplit[1] + versionsplit[2]
  version = "v1.9." + datumversion
  update()
  #aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
  #print(aktuelleZeit, ": Fallzahlen update")
  #f_update()
  archivDate = datumloop.date() - dt.timedelta(days=1)
  archiveDateStr = dt.datetime.strftime(archivDate, "%Y-%m-%d")
  meta_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "dataStore", "meta", "meta.json")
  metaArchivPath = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "archiv", "meta", archiveDateStr + "_" + "meta.json")
  if os.path.exists(meta_path):
    os.rename(meta_path, metaArchivPath)
  os.rename(metaNew_path, meta_path)
  os.system("git add .")
  os.system('git commit -m"update ' + datumversion + '"')
  endTime = dt.datetime.now()
  aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
  print(aktuelleZeit, ": total time for date:",datum, "=>", endTime - startTime)
  print("****************************************************")
  
