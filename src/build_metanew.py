import os, json, sys
import datetime as dt
import pandas as pd
from update_changes_history import update
import time
import utils as ut

def build_meta(datum):
  base_path = os.path.dirname(os.path.abspath(__file__))
  filename = "RKI_COVID19_XXXX-XX-XX.feather"
  filename = filename.replace("XXXX-XX-XX", datum)
  source_path = os.path.join(base_path, "..", "..", "RKIData", "feather", filename)
  source_path = os.path.normpath(source_path)
  date_time = dt.datetime.strptime(datum, "%Y-%m-%d")
  date_time_floored = dt.datetime.combine(date_time.date(), date_time.time().min).replace(tzinfo=dt.timezone.utc)
  unix_timestamp = int(dt.datetime.timestamp(date_time_floored)*1000)
    
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
    startDatum = dt.datetime.strftime(start.date(), "%Y-%m-%d")
    endDatum = dt.datetime.strftime(start.date(), "%Y-%m-%d")
  elif len(sys.argv) == 2:
    startDatum = sys.argv[1]
    endDatum = sys.argv[1]
  elif len(sys.argv) == 3:
    startDatum = sys.argv[1]
    endDatum = sys.argv[2]
  elif len(sys.argv) > 3:
    raise ValueError('not more than 2 arguments are allowed')
  startObject = dt.datetime.strptime(startDatum, '%Y-%m-%d')
  endObject = dt.datetime.strptime(endDatum, '%Y-%m-%d')
  
  base_path = os.path.dirname(os.path.abspath(__file__))
  aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
  print(f"{aktuelleZeit} : running from {startObject} to {endObject}")
  for datumloop in pd.date_range(start=startObject, end=endObject).tolist():
    startTime = dt.datetime.now()
    datum = datumloop.strftime('%Y-%m-%d')
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    print (f"{aktuelleZeit} : running on {datum}")
    new_meta = build_meta(datum)
    metaNew_path = os.path.join(base_path, "..", "dataStore", "meta", "meta_new.json")
    metaNew_path = os.path.normpath(metaNew_path)
    with open(metaNew_path, "w", encoding="utf8") as json_file:
      json.dump(new_meta, json_file, ensure_ascii=False)
    versionsplit = datum.split("-")
    datumversion = versionsplit[0] + versionsplit[1] + versionsplit[2]
    version = "v1.9." + datumversion
    filesToConvert = update()
    #aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    #print(aktuelleZeit, ": Fallzahlen update")
    #f_update()
    meta_path = os.path.join(base_path, "..", "dataStore", "meta", "meta.json")
    meta_path = os.path.normpath(meta_path)
    if os.path.exists(meta_path):
      os.remove(meta_path)
    os.rename(metaNew_path, meta_path)
    #os.system("git add .")
    #os.system('git commit -m"update ' + datumversion + '"')
    endTime = dt.datetime.now()
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    print(f"{aktuelleZeit} : total time for date: {datum} => {endTime - startTime}")
    print("****************************************************")
  print(f"convert all final feather files to json files, and write to disc")
  t1 = time.time()
  for featherfile, filename, path in filesToConvert:
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    print(f"{aktuelleZeit} : read  {featherfile}")
    df = ut.read_file(featherfile)
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    print(f"{aktuelleZeit} : write {os.path.join(path, filename)}")
    ut.write_json(df, filename, path)
  t2 = time.time()
  aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
  print(f"{aktuelleZeit} : done, all files written in {round(t2 - t1, 5)} secs")
  end = dt.datetime.now()
  print(f"{aktuelleZeit} : overall time for range {startObject} to {endObject} is => {end - start}")