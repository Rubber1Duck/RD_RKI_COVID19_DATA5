import os, json, sys, requests
import datetime as dt
import pandas as pd
from update_changes_history import update
import time
import utils as ut

def build_meta(datum):
  BL_filename = "BL_BaseData.feather"
  LK_filename = "LK_BaseData.feather"
  base_url = "https://raw.githubusercontent.com/Rubber1Duck/RD_RKI_COVID19_DATA/master/dataStore/"
  BL_url = base_url + "historychanges/" + BL_filename
  LK_url = base_url + "historychanges/" + LK_filename
  urlMeta = base_url + "meta/meta.json"
  # get meta.json
  meta_resp = requests.get(url=urlMeta, allow_redirects=True)
  metaObj = json.loads(meta_resp.content.decode(encoding="utf8"))
  if metaObj["version"] == datum:
    unix_timestamp = metaObj["modified"]
  else:
    date_time = dt.datetime.strptime(datum, "%Y-%m-%d")
    date_time_floored = dt.datetime.combine(date_time.date(), date_time.time().min).replace(tzinfo=dt.timezone.utc)
    unix_timestamp = int(dt.datetime.timestamp(date_time_floored)*1000)
  BL_size = requests.head(BL_url, allow_redirects=True).headers["content-length"]
  LK_size = requests.head(LK_url, allow_redirects=True).headers["content-length"]
    
  new_meta = {
    "publication_date": datum,
    "version": datum,
    "BL_filename": BL_filename,
    "BL_url": BL_url,
    "BL_size": BL_size,
    "LK_filename": LK_filename,
    "LK_url": LK_url,
    "LK_size": LK_size,
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
        
    filesToConvert = update(new_meta)
    
    meta_path = os.path.join(base_path, "..", "dataStore", "meta", "meta.json")
    meta_path = os.path.normpath(meta_path)
    if os.path.exists(meta_path):
      os.remove(meta_path)
    os.rename(metaNew_path, meta_path)
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