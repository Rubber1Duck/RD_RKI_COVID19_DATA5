import pandas as pd
import utils as ut
import os
import gc
import datetime as dt
import re

startTime = dt.datetime.now()
CV_dtypes = {"IdLandkreis": "str", "Altersgruppe": "str", "Geschlecht": "str", "NeuerFall": "Int32", "NeuerTodesfall": "Int32", "NeuGenesen": "Int32",
            "AnzahlFall": "Int32", "AnzahlTodesfall": "Int32", "AnzahlGenesen": "Int32", "Meldedatum": "object"}
base_path = os.path.dirname(os.path.abspath(__file__))
BV_csv_path = os.path.join(base_path, "..", "Bevoelkerung", "Bevoelkerung.csv")
BV_csv_path = os.path.normpath(BV_csv_path)
BV_dtypes = {"AGS": "str", "Altersgruppe": "str", "Name": "str", "GueltigAb": "object", "GueltigBis": "object", "Einwohner": "Int32", "männlich": "Int32", "weiblich": "Int32"}
data_path = os.path.join(base_path, "..", "..", "RKIData")
data_path = os.path.normpath(data_path)

# find all data file
iso_date_re = "([0-9]{4})(-?)(1[0-2]|0[1-9])\\2(3[01]|0[1-9]|[12][0-9])"
file_list = os.listdir(data_path)
file_list.sort(reverse=False)
pattern = "RKI_COVID19"
all_data_files = []
for file in file_list:
  file_path_full = os.path.join(data_path, file)
  if not os.path.isdir(file_path_full):
    filename = os.path.basename(file)
    re_filename = re.search(pattern, filename)
    re_search = re.search(iso_date_re, filename)
    if re_search and re_filename:
      report_date = dt.date(int(re_search.group(1)), int(re_search.group(3)), int(re_search.group(4))).strftime("%Y-%m-%d")
      all_data_files.append((file, file_path_full, report_date))

# open bevoelkerung.csv
BV = pd.read_csv(BV_csv_path, usecols=BV_dtypes.keys(), dtype=BV_dtypes)
BV["GueltigAb"] = pd.to_datetime(BV["GueltigAb"])
BV["GueltigBis"] = pd.to_datetime(BV["GueltigBis"])

for file, file_path_full, report_date in all_data_files:
  fileStartTime = dt.datetime.now()
  aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
  print(aktuelleZeit, ":", file)
  LK = pd.read_csv(file_path_full, engine="pyarrow", usecols=CV_dtypes.keys(), dtype=CV_dtypes)
  LK.sort_values(by=["IdLandkreis", "Altersgruppe", "Geschlecht", "Meldedatum"], axis=0, inplace=True, ignore_index=True)
  LK.reset_index(drop=True, inplace=True)
  # ----- Squeeze the dataframe to ideal memory size (see "compressing" Medium article and run_dataframe_squeeze.py for background)
  LK = ut.squeeze_dataframe(LK)
  aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
  print(aktuelleZeit, ":",LK.shape[0],"rows")
  Datenstand = dt.datetime.strptime(report_date, "%Y-%m-%d")
  print(aktuelleZeit, ": add missing columns ...")
  LK["IdLandkreis"] = LK["IdLandkreis"].astype(str).str.zfill(5)
  LK.insert(loc=0, column="IdBundesland", value=LK["IdLandkreis"].str[:-3].copy())
  LK["Meldedatum"] = pd.to_datetime(LK["Meldedatum"]).dt.date
  LK.insert(loc=0, column="Datenstand", value=Datenstand.date())
  # add Bundesland und Landkreis
  LK.insert(loc=2, column="Bundesland", value="")
  LK.insert(loc=4, column="Landkreis", value="")
  BV_mask = ((BV["AGS"].isin(LK["IdBundesland"])) & (BV["Altersgruppe"] == "A00+") & (BV["GueltigAb"] <= Datenstand) & (BV["GueltigBis"] >= Datenstand))
  BV_masked = BV[BV_mask].copy()
  BV_masked.drop(["GueltigAb", "GueltigBis", "Altersgruppe", "Einwohner", "männlich", "weiblich"], inplace=True, axis=1)
  ID = LK["IdBundesland"].copy()
  ID = pd.merge(left=ID, right=BV_masked, left_on="IdBundesland", right_on="AGS", how="left")
  LK["Bundesland"] = ID["Name"].copy()
  BV_mask = ((BV["AGS"].isin(LK["IdLandkreis"])) & (BV["Altersgruppe"] == "A00+") & (BV["GueltigAb"] <= Datenstand) & (BV["GueltigBis"] >= Datenstand))
  BV_masked = BV[BV_mask].copy()
  BV_masked.drop(["GueltigAb", "GueltigBis", "Altersgruppe", "Einwohner", "männlich", "weiblich"], inplace=True, axis=1)
  ID = LK["IdLandkreis"].copy()
  ID = pd.merge(left=ID, right=BV_masked, left_on="IdLandkreis", right_on="AGS", how="left")
  LK["Landkreis"] = ID["Name"].copy()
  ID = pd.DataFrame()
  del ID
  gc.collect()
  LK.insert(loc=0, column="IdStaat", value="00")
  LK = LK[LK["Landkreis"].notna()]
  LK.reset_index(inplace=True, drop=True)
  # ----- Squeeze the dataframe to ideal memory size (see "compressing" Medium article and run_dataframe_squeeze.py for background)
  LK = ut.squeeze_dataframe(LK)
  # store dataBase to feather file to save memory
  filename = "RKI_COVID19_" + report_date + ".feather"
  feather_path = os.path.join(data_path, filename)
  feather_path = os.path.normpath(feather_path) 
  ut.write_file(df=LK, fn=feather_path, compression="lz4")
  aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
  fileEndTime = dt.datetime.now()
  print(aktuelleZeit, ": Time for", file, fileEndTime - fileStartTime)
endTime = dt.datetime.now()
print(aktuelleZeit, ": Total Time for all files", endTime - startTime)
