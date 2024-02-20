import os, json, sys
import datetime as dt
import pandas as pd
from update_changes_history import update

def build_meta(datum):
  base_path = os.path.dirname(os.path.abspath(__file__))
  filename = "RKI_COVID19_XXXX-XX-XX.feather"
  filename = filename.replace("XXXX-XX-XX", datum)
  source_path = os.path.join(base_path, "..", "..", "RKIData", "feather", filename)
  source_path = os.path.normpath(source_path)
  date_time = dt.datetime.strptime(datum, "%Y-%m-%d")
  unix_timestamp = int(dt.datetime.timestamp(date_time)*1000)
  
  new_meta = {
    "publication_date": datum,
    "version": datum,
    "size": os.path.getsize(source_path),
    "filename": filename,
    "url": source_path,
    "modified": unix_timestamp}
  
  return new_meta

if __name__ == '__main__':
  start = dt.datetime.now()
  if len(sys.argv) == 1:
    # for debug put startdate here
    startDatum = "2020-04-08"
  else:
    # for debug put enddate here
    startDatum = sys.argv[1]
  startObject = dt.datetime.strptime(startDatum, '%Y-%m-%d')
  if len(sys.argv) == 1:
    endDatum = "2020-04-09"
  else:
    endDatum = sys.argv[2]
  endObject = dt.datetime.strptime(endDatum, '%Y-%m-%d')
  base_path = os.path.dirname(os.path.abspath(__file__))
  aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
  print(aktuelleZeit, ": running from", startObject, "to", endObject)
  for datumloop in pd.date_range(start=startObject, end=endObject).tolist():
    startTime = dt.datetime.now()
    datum = datumloop.strftime('%Y-%m-%d')
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    print (aktuelleZeit, ": running on", datum)
    new_meta = build_meta(datum)
    metaNew_path = os.path.join(base_path, "..", "dataStore", "meta", "meta_new.json")
    metaNew_path = os.path.normpath(metaNew_path)
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
    meta_path = os.path.join(base_path, "..", "dataStore", "meta", "meta.json")
    meta_path = os.path.normpath(meta_path)
    metaArchivPath = os.path.join(base_path, "..", "archiv", "meta", archiveDateStr + "_" + "meta.json")
    metaArchivPath = os.path.normpath(metaArchivPath)
    if os.path.exists(meta_path):
      os.rename(meta_path, metaArchivPath)
    os.rename(metaNew_path, meta_path)
    #os.system("git add .")
    #os.system('git commit -m"update ' + datumversion + '"')
    endTime = dt.datetime.now()
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    print(aktuelleZeit, ": total time for date:",datum, "=>", endTime - startTime)
    print("****************************************************")
  end = dt.datetime.now()
  print(aktuelleZeit, "overall time for range", startObject, "to", endObject, "is =>", end-start)