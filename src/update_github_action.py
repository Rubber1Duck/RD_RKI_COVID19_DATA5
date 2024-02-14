import os
import datetime as dt
import numpy as np
import pandas as pd
import json
import utils as ut
import gc

def update():
    startTime = dt.datetime.now()
    base_path = os.path.dirname(os.path.abspath(__file__))
    meta_path = os.path.join(base_path, "..", "dataStore", "meta")
    filename_meta = "meta_new.json"
    actual_meta = "meta.json"
    feather_path = os.path.join(base_path, "..", "dataBase.feather")
    BV_csv_path = os.path.join(base_path, "..", "Bevoelkerung", "Bevoelkerung.csv")
    LK_dtypes = {"Datenstand": "object", "IdLandkreis": "str", "Landkreis": "str", "incidence_7d": "float64"}
    LK_dtypes_single_files = {"Datenstand": "object", "IdLandkreis": "str", "Landkreis": "str", "AnzahlFall_7d": "int32", "incidence_7d": "float64"}
    BL_dtypes = {"Datenstand": "object", "IdBundesland": "str", "Bundesland": "str", "incidence_7d": "float64"}
    kum_dtypes = {"D": "object", "I": "str", "T": "str", "i": "float64"}
    BV_dtypes = {"AGS": "str", "Altersgruppe": "str", "Name": "str", "GueltigAb": "object", "GueltigBis": "object", "Einwohner": "Int32", "männlich": "Int32", "weiblich": "Int32"}
    CV_dtypes = {"IdLandkreis": "str", "Altersgruppe": "str", "Geschlecht": "str", "NeuerFall": "Int32", "NeuerTodesfall": "Int32", "NeuGenesen": "Int32",
                 "AnzahlFall": "Int32", "AnzahlTodesfall": "Int32", "AnzahlGenesen": "Int32", "Meldedatum": "object"}
    # open bevoelkerung.csv
    BV = pd.read_csv(BV_csv_path, usecols=BV_dtypes.keys(), dtype=BV_dtypes)
    BV["GueltigAb"] = pd.to_datetime(BV["GueltigAb"])
    BV["GueltigBis"] = pd.to_datetime(BV["GueltigBis"])
    BV_BL = BV[BV["AGS"].str.len() == 2].copy()
    BV_BL.reset_index(inplace=True, drop=True)
    BV_BL_A00 = BV_BL[BV_BL["Altersgruppe"] == "A00+"].copy()
    BV_BL_A00.reset_index(inplace=True, drop=True)
    BV_LK = BV[BV["AGS"].str.len() == 5].copy()
    BV_LK.reset_index(inplace=True, drop=True)
    BV_LK_A00 = BV_LK[BV_LK["Altersgruppe"] == "A00+"].copy()
    BV_LK_A00.reset_index(inplace=True, drop=True)
    # ----- Squeeze the dataframe to ideal memory size (see "compressing" Medium article and run_dataframe_squeeze.py for background)
    BV = ut.squeeze_dataframe(BV)
    BV_BL = ut.squeeze_dataframe(BV_BL)
    BV_BL_A00 = ut.squeeze_dataframe(BV_BL_A00)
    BV_LK = ut.squeeze_dataframe(BV_LK)
    BV_LK_A00 = ut.squeeze_dataframe(BV_LK_A00)
    # load covid latest from web
    with open(meta_path + "/" + filename_meta, "r", encoding="utf8") as file:
        metaObj = json.load(file)
    fileNameOrig = metaObj["filename"]
    fileSize = int(metaObj["size"])
    url = metaObj["url"]
    timeStamp = metaObj["modified"]
    Datenstand = dt.datetime.fromtimestamp(timeStamp / 1000)
    Datenstand = Datenstand.replace(hour=0, minute=0, second=0, microsecond=0)
    try:
        with open(meta_path + "/" + actual_meta, "r", encoding="utf8") as file:
            metaActual = json.load(file)
        oldDatenstandStr = dt.datetime.strptime(dt.datetime.fromtimestamp(metaActual["timestamp"] / 1000), "%Y-%m-%d")
    except:
        oldDatenstand = Datenstand.date() - dt.timedelta(days=1)
        oldDatenstandStr = dt.datetime.strftime(oldDatenstand, "%Y-%m-%d")
    filedate = (dt.datetime.fromtimestamp(metaObj["modified"] / 1000).date().strftime("%Y-%m-%d"))
    fileSizeMb = round(fileSize / 1024 / 1024, 1)
    fileNameRoot = "RKI_COVID19_"
    fileName = fileNameRoot + filedate + ".csv"
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    print(aktuelleZeit, ": loading", fileNameOrig, "(size:", fileSize, "bytes =", fileSizeMb,
          "MegaByte)")
    # for testing or fixing uncommend the following lines and set the values
    # path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data')
    # testfile = os.path.join(path, '2023-12-26_Deutschland_SarsCov2_Infektionen.csv.xz')
    # LK = pd.read_csv(testfile, usecols=CV_dtypes.keys(), dtype=CV_dtypes)
    # Datenstand = dt.datetime(year=2023, month=12, day=26, hour=0, minute=0, second=0, microsecond=0)
    # fileName = "RKI_COVID19_2023-12-26.csv"
    LK = pd.read_csv(url, usecols=CV_dtypes.keys(), dtype=CV_dtypes)
    LK.sort_values(by=["IdLandkreis", "Altersgruppe", "Geschlecht", "Meldedatum"], axis=0, inplace=True, ignore_index=True)
    LK.reset_index(drop=True, inplace=True)
    # ----- Squeeze the dataframe to ideal memory size (see "compressing" Medium article and run_dataframe_squeeze.py for background)
    LK = ut.squeeze_dataframe(LK)
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    print(aktuelleZeit, ":",LK.shape[0],"rows")
    data_path = os.path.join(base_path, "..", "data")
    fileNameXz = fileName + ".xz"
    full_path = os.path.join(base_path, data_path, fileName)
    full_pathXz = os.path.join(base_path, data_path, fileNameXz)
    data_path = os.path.normpath(data_path)
    full_path = os.path.normpath(full_path)
    istDatei = os.path.isfile(full_path)
    istDateiXz = os.path.isfile(full_pathXz)
    if not (istDatei | istDateiXz):
        print(aktuelleZeit, ": writing DataFrame to", fileName, "...")
        LK.to_csv(full_path, index=False, header=True, lineterminator="\n", encoding="utf-8", date_format="%Y-%m-%d", columns=CV_dtypes.keys())
    else:
        if istDatei:
            fileExists = fileName
        else:
            fileExists = fileNameXz
        aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
        print(aktuelleZeit, ":", fileExists, "already exists.")
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
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
    ut.write_file(df=LK, fn=feather_path, compression="lz4")
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    
    # ageGroup Data
    print(aktuelleZeit, ": calculating age-group data ...")
    # kopiere dataBase ohne unbekannte Altersgruppen oder unbekannte Geschlechter
    LK = LK[LK["Altersgruppe"] != "unbekannt"].copy()
    LK = LK[LK["Geschlecht"] != "unbekannt"].copy()
    # korrigiere Kategorien Altersgruppe und Geschlecht
    LK["Geschlecht"] = LK["Geschlecht"].cat.remove_unused_categories()
    LK["Altersgruppe"] = LK["Altersgruppe"].cat.remove_unused_categories()
    LK.reset_index(inplace=True, drop=True)
    # lösche alle nicht benötigten Spalten
    LK.drop(["Bundesland", "Landkreis", "Meldedatum", "Datenstand", "IdStaat"], inplace=True, axis=1)
    # used keylists
    key_list_LK_age = ["IdLandkreis", "Altersgruppe"]
    key_list_BL_age = ["IdBundesland", "Altersgruppe"]
    key_list_ID0_age = ["Altersgruppe"]
    # calculate the age group data
    LK["AnzahlFall"] = np.where(LK["NeuerFall"].isin([1, 0]), LK["AnzahlFall"], 0).astype(int)
    LK["casesMale"] = np.where(LK["Geschlecht"] == "M", LK["AnzahlFall"], 0).astype(int)
    LK["casesFemale"] = np.where(LK["Geschlecht"] == "W", LK["AnzahlFall"], 0).astype(int)
    LK["AnzahlTodesfall"] = np.where(LK["NeuerTodesfall"].isin([1, 0, -9]), LK["AnzahlTodesfall"], 0).astype(int)
    LK["deathsMale"] = np.where(LK["Geschlecht"] == "M", LK["AnzahlTodesfall"], 0).astype(int)
    LK["deathsFemale"] = np.where(LK["Geschlecht"] == "W", LK["AnzahlTodesfall"], 0).astype(int)
    LK.drop(["NeuGenesen", "NeuerFall", "NeuerTodesfall", "AnzahlFall", "AnzahlTodesfall", "AnzahlGenesen", "Geschlecht"], inplace=True, axis=1)
    agg_key = {
        c: "max" if c in ["IdBundesland"] else "sum"
        for c in LK.columns
        if c not in key_list_LK_age
    }
    LK = LK.groupby(by=key_list_LK_age, as_index=False, observed=True).agg(agg_key)
    LK.reset_index(inplace=True, drop=True)
    LK_pop_mask = ((BV["AGS"].isin(LK["IdLandkreis"])) & (BV["Altersgruppe"].isin(LK["Altersgruppe"])) & (BV["GueltigAb"] <= Datenstand) & (BV["GueltigBis"] >= Datenstand))
    LK_pop = BV[LK_pop_mask]
    LK_pop.reset_index(inplace=True, drop=True)
    LK["populationM"] = LK_pop["männlich"].astype(int)
    LK["populationW"] = LK_pop["weiblich"].astype(int)
    LK["casesMale"] = LK["casesMale"].astype(int)
    LK["casesFemale"] = LK["casesFemale"].astype(int)
    LK["deathsMale"] = LK["deathsMale"].astype(int)
    LK["deathsFemale"] = LK["deathsFemale"].astype(int)
    LK["casesMalePer100k"] = round(LK["casesMale"] / LK["populationM"] * 100000, 1)
    LK["casesFemalePer100k"] = round(LK["casesFemale"] / LK["populationW"] * 100000, 1)
    LK["deathsMalePer100k"] = round(LK["deathsMale"] / LK["populationM"] * 100000, 1)
    LK["deathsFemalePer100k"] = round(LK["deathsFemale"] / LK["populationW"] * 100000, 1)
    agg_key = {
        c: "max" if c in ["IdLandkreis"] else "sum"
        for c in LK.columns
        if c not in key_list_BL_age
    }
    BL = LK.groupby(by=key_list_BL_age, as_index=False, observed=False).agg(agg_key).copy()
    BL["casesMalePer100k"] = round(BL["casesMale"] / BL["populationM"] * 100000, 1)
    BL["casesFemalePer100k"] = round(BL["casesFemale"] / BL["populationW"] * 100000, 1)
    BL["deathsMalePer100k"] = round(BL["deathsMale"] / BL["populationM"] * 100000, 1)
    BL["deathsFemalePer100k"] = round(BL["deathsFemale"] / BL["populationW"] * 100000, 1)
    BL.drop(["IdLandkreis"], inplace=True, axis=1)
    agg_key = {
        c: "max" if c in ["IdBundesland", "IdLandkreis"] else "sum"
        for c in BL.columns
        if c not in key_list_ID0_age
    }
    ID0 = BL.groupby(by=key_list_ID0_age, as_index=False, observed=False).agg(agg_key).copy()
    ID0["IdBundesland"] = "00"
    ID0["casesMalePer100k"] = round(ID0["casesMale"] / ID0["populationM"] * 100000, 1)
    ID0["casesFemalePer100k"] = round(ID0["casesFemale"] / ID0["populationW"] * 100000, 1)
    ID0["deathsMalePer100k"] = round(ID0["deathsMale"] / ID0["populationM"] * 100000, 1)
    ID0["deathsFemalePer100k"] = round(ID0["deathsFemale"] / ID0["populationW"] * 100000, 1)
    LK.drop(["populationM", "populationW", "IdBundesland"], inplace=True, axis=1)
    BL.drop(["populationM", "populationW"], inplace=True, axis=1)
    ID0.drop(["populationM", "populationW"], inplace=True, axis=1)
    BL = pd.concat([ID0, BL])
    BL.reset_index(inplace=True, drop=True)
    
    # store
    path = os.path.join(base_path, "..", "dataStore", "agegroup")
    archivPath = os.path.join(base_path, "..", "archiv", "agegroup")
    ut.write_json(LK, "districts.json", path, oldDatenstandStr, archivPath)
    ut.write_json(BL, "states.json", path, oldDatenstandStr, archivPath)
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
        
    # accumulated and new cases, deaths, recovered, casesPerWeek, deathsPerWeek
    # add country column
    print(aktuelleZeit, ": calculating new and accumulated data ...")
    LK = ut.read_file(fn=feather_path)
    # used keylists
    key_list_LK_cases = ["IdLandkreis"]
    key_list_BL_cases = ["IdBundesland"]
    key_list_ID0_cases = ["IdStaat"]
    # calculate the values
    LK["accuCases"] = np.where(LK["NeuerFall"].isin([1, 0]), LK["AnzahlFall"], 0).astype(int)
    LK["newCases"] = np.where(LK["NeuerFall"].isin([1, -1]), LK["AnzahlFall"], 0).astype(int)
    LK["accuCasesPerWeek"] = np.where(LK["Meldedatum"] > (Datenstand.date() - dt.timedelta(days=8)), LK["accuCases"], 0).astype(int)
    LK["newCasesPerWeek"] = np.where(LK["Meldedatum"] > (Datenstand.date() - dt.timedelta(days=8)), LK["newCases"], 0).astype(int)
    LK["accuDeaths"] = np.where(LK["NeuerTodesfall"].isin([1, 0, -9]), LK["AnzahlTodesfall"], 0).astype(int)
    LK["newDeaths"] = np.where(LK["NeuerTodesfall"].isin([1, -1]), LK["AnzahlTodesfall"], 0).astype(int)
    LK["accuDeathsPerWeek"] = np.where(LK["Meldedatum"] > (Datenstand.date() - dt.timedelta(days=8)), LK["accuDeaths"], 0).astype(int)
    LK["newDeathsPerWeek"] = np.where(LK["Meldedatum"] > (Datenstand.date() - dt.timedelta(days=8)), LK["newDeaths"], 0).astype(int)
    LK["accuRecovered"] = np.where(LK["NeuGenesen"].isin([1, 0]), LK["AnzahlGenesen"], 0).astype(int)
    LK["newRecovered"] = np.where(LK["NeuGenesen"].isin([1, -1]), LK["AnzahlGenesen"], 0).astype(int)
    LK.drop(["NeuGenesen", "NeuerFall", "NeuerTodesfall", "AnzahlFall", "AnzahlTodesfall", "AnzahlGenesen", "Altersgruppe", "Geschlecht"], inplace=True, axis=1)
    agg_key = {
        c: "max"
        if c in ["IdStaat", "IdBundesland", "Meldedatum", "Datenstand", "Landkreis", "Bundesland"]
        else "sum"
        for c in LK.columns
        if c not in key_list_LK_cases
    }
    LK = LK.groupby(by=key_list_LK_cases, as_index=False, observed=False).agg(agg_key)
    agg_key = {
        c: "max"
        if c in ["IdStaat", "Meldedatum", "Datenstand", "Bundesland", "IdLandkreis", "Landkreis"]
        else "sum"
        for c in LK.columns
        if c not in key_list_BL_cases
    }
    BL = LK.groupby(by=key_list_BL_cases, as_index=False, observed=False).agg(agg_key)
    agg_key = {
        c: "max"
        if c in ["Meldedatum", "Datenstand", "Bundesland", "IdLandkreis", "Landkreis", "IdBundesland"]
        else "sum"
        for c in BL.columns
        if c not in key_list_ID0_cases
    }
    ID0 = BL.groupby(by=key_list_ID0_cases, as_index=False, observed=False).agg(agg_key)
    LK.drop(["IdStaat", "IdBundesland"], inplace=True, axis=1)
    LK_pop_mask = ((BV["AGS"].isin(LK["IdLandkreis"])) & (BV["Altersgruppe"] == "A00+") & (BV["GueltigAb"] <= Datenstand) & (BV["GueltigBis"] >= Datenstand))
    LK_pop = BV[LK_pop_mask]
    LK_pop.reset_index(inplace=True, drop=True)
    LK["population"] = LK_pop["Einwohner"]
    BL.drop(["IdStaat", "IdLandkreis", "Landkreis"], inplace=True, axis=1)
    ID0.drop(["IdStaat", "IdLandkreis", "Landkreis"], inplace=True, axis=1)
    ID0["IdBundesland"] = "00"
    ID0["Bundesland"] = "Bundesgebiet"
    BL = pd.concat([ID0, BL])
    BL.reset_index(inplace=True, drop=True)
    BL_pop_mask = ((BV["AGS"].isin(BL["IdBundesland"])) & (BV["Altersgruppe"] == "A00+") & (BV["GueltigAb"] <= Datenstand) & (BV["GueltigBis"] >= Datenstand))
    BL_pop = BV[BL_pop_mask]
    BL_pop.reset_index(inplace=True, drop=True)
    BL["population"] = BL_pop["Einwohner"]
    
    # store
    path = os.path.join(base_path, "..", "dataStore", "cases")
    archivPath = os.path.join(base_path, "..", "archiv", "cases")
    ut.write_json(LK, "districts.json", path, oldDatenstandStr, archivPath)
    ut.write_json(BL, "states.json", path, oldDatenstandStr, archivPath)
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    
    # History
    # DistrictCasesHistory, DistrictDeathsHistory, DistrictRecoveredHistory
    # StateCasesHistory, StateDeathsHistory, StateRecoveredHistory
    print(aktuelleZeit, ": calculating history data ...")
    LK = ut.read_file(fn=feather_path)
    LK.drop(["IdStaat"], inplace=True, axis=1)
    # used keylists
    key_list_LK_hist = ["IdLandkreis", "Meldedatum"]
    key_list_BL_hist = ["IdBundesland", "Meldedatum"]
    key_list_ID0_hist = ["Meldedatum"]
    LK["AnzahlFall"] = np.where(LK["NeuerFall"].isin([1, 0]), LK["AnzahlFall"], 0).astype(int)
    LK["AnzahlTodesfall"] = np.where(LK["NeuerTodesfall"].isin([1, 0, -9]), LK["AnzahlTodesfall"], 0).astype(int)
    LK["AnzahlGenesen"] = np.where(LK["NeuGenesen"].isin([1, 0, -9]), LK["AnzahlGenesen"], 0).astype(int)
    LK.drop(["NeuerFall", "NeuerTodesfall", "NeuGenesen", "Altersgruppe", "Geschlecht", "Datenstand"], inplace=True, axis=1)
    LK.rename(columns={"AnzahlFall": "cases", "AnzahlTodesfall": "deaths", "AnzahlGenesen": "recovered"}, inplace=True)
    agg_key = {
        c: "max"
        if c in ["IdBundesland", "Datenstand", "Landkreis", "Bundesland"]
        else "sum"
        for c in LK.columns
        if c not in key_list_LK_hist
    }
    LK = LK.groupby(by=key_list_LK_hist, as_index=False, observed=True).agg(agg_key)
    agg_key = {
        c: "max"
        if c in ["IdLandkreis", "Datenstand", "Landkreis", "Bundesland"]
        else "sum"
        for c in LK.columns
        if c not in key_list_BL_hist
    }
    BL = LK.groupby(by=key_list_BL_hist, as_index=False, observed=True).agg(agg_key)
    agg_key = {
        c: "max"
        if c in ["IdBundesland", "IdLandkreis", "Datenstand", "Bundesland", "Landkreis"]
        else "sum"
        for c in BL.columns
        if c not in key_list_ID0_hist
    }
    ID0 = BL.groupby(by=key_list_ID0_hist, as_index=False, observed=True).agg(agg_key)
    LK.drop(["IdBundesland", "Bundesland"], inplace=True, axis=1)
    LK.sort_values(by=key_list_LK_hist, inplace=True)
    LK.reset_index(inplace=True, drop=True)
    BL.drop(["IdLandkreis", "Landkreis"], inplace=True, axis=1)
    ID0.drop(["IdLandkreis", "Landkreis"], inplace=True, axis=1)
    ID0["IdBundesland"] = "00"
    ID0["Bundesland"] = "Bundesgebiet"
    BL = pd.concat([ID0, BL])
    BL.sort_values(by=key_list_BL_hist, inplace=True)
    BL.reset_index(inplace=True, drop=True)
    LK["Meldedatum"] = pd.to_datetime(LK["Meldedatum"]).dt.date
    BL["Meldedatum"] = pd.to_datetime(BL["Meldedatum"]).dt.date
    # fill dates for every region
    startDate = "2020-01-01"
    date_range_str = []
    for datum in pd.date_range(end=(Datenstand - dt.timedelta(days=1)), start=startDate).to_list():
        date_range_str.append(datum.strftime("%Y-%m-%d"))
    allDates = pd.DataFrame(date_range_str, columns=["Datum"])
    BL_ID = pd.DataFrame(pd.unique(BL["IdBundesland"]).copy(), columns=["IdBundesland"])
    LK_ID = pd.DataFrame(pd.unique(LK["IdLandkreis"]).copy(), columns=["IdLandkreis"])
    # add Bundesland, Landkreis and Einwohner
    BL_ID.insert(loc=1, column="Bundesland", value="")
    BL_ID.insert(loc=2, column="Einwohner", value="")
    LK_ID.insert(loc=1, column="Landkreis", value="")
    LK_ID.insert(loc=2, column="Einwohner", value="")
    BV_mask = ((BV_BL_A00["AGS"].isin(BL_ID["IdBundesland"])) & (BV_BL_A00["GueltigAb"] <= Datenstand) & (BV_BL_A00["GueltigBis"] >= Datenstand))
    BV_masked = BV_BL_A00[BV_mask].copy()
    BV_masked.drop(["GueltigAb", "GueltigBis", "Altersgruppe", "männlich", "weiblich"], inplace=True, axis=1)
    ID = BL_ID["IdBundesland"].copy()
    ID = pd.merge(left=ID, right=BV_masked, left_on="IdBundesland", right_on="AGS", how="left")
    BL_ID["Bundesland"] = ID["Name"].copy()
    BL_ID["Einwohner"] = ID["Einwohner"].copy()
    BV_mask = ((BV_LK_A00["AGS"].isin(LK["IdLandkreis"])) & (BV_LK_A00["GueltigAb"] <= Datenstand) & (BV_LK_A00["GueltigBis"] >= Datenstand))
    BV_masked = BV_LK_A00[BV_mask].copy()
    BV_masked.drop(["GueltigAb", "GueltigBis", "Altersgruppe", "männlich", "weiblich"], inplace=True, axis=1)
    ID = LK_ID["IdLandkreis"].copy()
    ID = pd.merge(left=ID, right=BV_masked, left_on="IdLandkreis", right_on="AGS", how="left")
    LK_ID["Landkreis"] = ID["Name"].copy()
    LK_ID["Einwohner"] = ID["Einwohner"].copy()
    BL_Dates = BL_ID.merge(allDates, how="cross")
    BL_Dates = ut.squeeze_dataframe(BL_Dates)
    LK_Dates = LK_ID.merge(allDates, how="cross")
    LK_Dates = ut.squeeze_dataframe(LK_Dates)
    BL_Dates["Datum"] = pd.to_datetime(BL_Dates["Datum"]).dt.date
    LK_Dates["Datum"] = pd.to_datetime(LK_Dates["Datum"]).dt.date
    BL = BL.merge(BL_Dates, how="right", left_on=["IdBundesland", "Meldedatum"], right_on=["IdBundesland", "Datum"])
    BL["Bundesland_x"] = BL["Bundesland_y"]
    BL["Meldedatum"] = BL["Datum"]
    BL.rename({"Bundesland_x": "Bundesland"}, inplace=True, axis=1)
    BL.drop(["Bundesland_y", "Datum"], inplace=True, axis=1)
    LK = LK.merge(LK_Dates, how="right", left_on=["IdLandkreis", "Meldedatum"], right_on=["IdLandkreis", "Datum"])
    LK["Landkreis_x"] = LK["Landkreis_y"]
    LK["Meldedatum"] = LK["Datum"]
    LK.rename({"Landkreis_x": "Landkreis"}, inplace=True, axis=1)
    LK.drop(["Landkreis_y", "Datum"], inplace=True, axis=1)
    BL["cases"] = BL["cases"].fillna(0).astype(int)
    BL["deaths"] = BL["deaths"].fillna(0).astype(int)
    BL["recovered"] = BL["recovered"].fillna(0).astype(int)
    LK["cases"] = LK["cases"].fillna(0).astype(int)
    LK["deaths"] = LK["deaths"].fillna(0).astype(int)
    LK["recovered"] = LK["recovered"].fillna(0).astype(int)
    BL["Meldedatum"] = BL["Meldedatum"].astype(str)
    BL.insert(loc=7, column="cases7d", value=0)
    BL.insert(loc=8, column="incidence7d", value=0.0)
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    print(aktuelleZeit, ":   |-calculating BL incidence history data ...")
    unique_BLID = BL_ID["IdBundesland"].unique()
    BL_I = ut.calc_incidence_BL(df=BL, unique_ID=unique_BLID)
    BL["cases7d"] = BL_I["cases7d"]
    BL["incidence7d"] = BL_I["incidence7d"].round(5)
    BL.drop(["Einwohner"], inplace=True, axis=1)
    BL_I = pd.DataFrame()
    BLID = pd.DataFrame()
    BL_ID = pd.DataFrame()
    BL_Dates = pd.DataFrame()
    del BL_I
    del BLID
    del BL_ID
    del BL_Dates
    gc.collect()
    LK["Meldedatum"] = LK["Meldedatum"].astype(str)
    LK.insert(loc=7, column="cases7d", value=0)
    LK.insert(loc=8, column="incidence7d", value=0.0)
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    print(aktuelleZeit, ":   |-calculating LK incidence history data ...")
    unique_LKID = LK_ID.IdLandkreis.unique()
    LK_I = ut.calc_incidence_LK(df=LK, unique_ID=unique_LKID)
    LK["cases7d"] = LK_I["cases7d"]
    LK["incidence7d"] = LK_I["incidence7d"].round(5)
    LK.drop(["Einwohner"], inplace=True, axis=1)
    LK_I = pd.DataFrame()
    LKID = pd.DataFrame()
    LK_ID = pd.DataFrame()
    LK_Dates = pd.DataFrame()
    del LK_I
    del LKID
    del LK_ID
    del LK_Dates
    gc.collect()
        
    # store
    path = os.path.join(base_path, "..", "dataStore", "history")
    archivPath = os.path.join(base_path, "..", "archiv", "history")
    LKHistoryFeatherFileName = "districts_cases.feather"
    BLHistoryFeatherFileName = "states_cases.feather"
    LKHistoryFeatherFullPath = os.path.join(base_path, "..", "dataStore", "history", LKHistoryFeatherFileName)
    BLHistoryFeatherFullPath = os.path.join(base_path, "..", "dataStore", "history", BLHistoryFeatherFileName)
    # complete districts (cases, deaths, recovered. incidence)
    #ut.write_json(LK, "districts.json", path, oldDatenstandStr, archivPath)
    # complete states (cases, deaths, recovered. incidence)
    #ut.write_json(BL, "states.json", path, oldDatenstandStr, archivPath)
    # complete districts (cases, deaths, recovered. incidence) short
    LK.rename(columns={"IdLandkreis": "i", "Landkreis": "t", "Meldedatum": "m", "cases": "c", "deaths": "d", "recovered": "r", "cases7d": "c7", "incidence7d": "i7"}, inplace=True)
    ut.write_json(LK, "districts_new.json", path, oldDatenstandStr, archivPath)
    # complete states (cases, deaths, recovered. incidence) short
    BL.rename(columns={"IdBundesland": "i", "Bundesland": "t", "Meldedatum": "m", "cases": "c", "deaths": "d", "recovered": "r", "cases7d": "c7", "incidence7d": "i7"}, inplace=True)
    ut.write_json(BL, "states_new.json", path, oldDatenstandStr, archivPath)
    # calculate cases diff
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    print(aktuelleZeit, ": calculating history difference")
    LK.drop(["d", "r", "c7", "i7"], inplace=True, axis=1)
    BL.drop(["d", "r", "c7", "i7"], inplace=True, axis=1)
    if os.path.exists(LKHistoryFeatherFullPath):
        oldLK = ut.read_file(fn=LKHistoryFeatherFullPath)
    ut.write_file(df=LK, fn=LKHistoryFeatherFullPath, compression="lz4")
    if os.path.exists(BLHistoryFeatherFullPath):
        oldBL = ut.read_file(fn=BLHistoryFeatherFullPath)
    ut.write_file(df=BL, fn=BLHistoryFeatherFullPath, compression="lz4")
    try:
        LKDiff = ut.get_different_rows(oldLK, LK)
    except:
        LKDiff = LK.copy()
    try:
        BLDiff = ut.get_different_rows(oldBL, BL)
    except:
        BLDiff = BL.copy()
    LKDiff["cD"] = dt.datetime.strftime(Datenstand, "%Y-%m-%d")
    BLDiff["cD"] = dt.datetime.strftime(Datenstand, "%Y-%m-%d")
    LKDiffFullFeatherPath = os.path.join(base_path, "..", "dataStore", "history", "districts_cases_Diff.feather")
    BLDiffFullFeatherPath = os.path.join(base_path, "..", "dataStore", "history", "states_cases_Diff.feather")
    path = os.path.join(base_path, "..", "dataStore", "history")
    if os.path.exists(LKDiffFullFeatherPath):
        LKoldDiff = ut.read_file(LKDiffFullFeatherPath)
        LKDiff = pd.concat([LKoldDiff, LKDiff])
        LKDiff.sort_values(by=["i", "m", "cD"], inplace=True)
        LKDiff.reset_index(inplace=True, drop=True)
    ut.write_file(LKDiff, LKDiffFullFeatherPath, compression="lz4")
    ut.write_json(LKDiff, "districts_cases_Diff.json", path)
    if os.path.exists(BLDiffFullFeatherPath):
        BLoldDiff = ut.read_file(BLDiffFullFeatherPath)
        BLDiff = pd.concat([BLoldDiff, BLDiff])
        BLDiff.sort_values(by=["i", "m", "cD"], inplace=True)
        BLDiff.reset_index(inplace=True, drop=True)
    ut.write_file(BLDiff, BLDiffFullFeatherPath, compression="lz4")
    ut.write_json(BLDiff, "states_cases_Diff.json", path)

    # *******************
    # * fixed-incidence *
    # *******************
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    print(aktuelleZeit, ": calculating fixed-incidence data ...")
    LK = ut.read_file(fn=feather_path)
    # used keylists
    key_list_LK_fix = ["IdStaat", "IdBundesland", "IdLandkreis"]
    key_list_BL_fix = ["IdStaat", "IdBundesland"]
    key_list_ID0_fix = ["IdStaat"]
    LK["AnzahlFall"] = np.where(LK["NeuerFall"].isin([0, 1]), LK["AnzahlFall"], 0).astype(int)
    LK["AnzahlFall_7d"] = np.where(LK["Meldedatum"] > (Datenstand.date() - dt.timedelta(days=8)), LK["AnzahlFall"], 0).astype(int)
    LK.drop(["Meldedatum", "NeuerFall", "NeuerTodesfall", "AnzahlFall", "AnzahlTodesfall", "Landkreis", "Bundesland", "NeuGenesen", "AnzahlGenesen", "Altersgruppe", "Geschlecht"], inplace=True, axis=1)
    agg_key = {
        c: "max" if c in ["Datenstand"] else "sum"
        for c in LK.columns
        if c not in key_list_LK_fix
    }
    LK = LK.groupby(by=key_list_LK_fix, as_index=False, observed=True).agg(agg_key)
    agg_key = {
        c: "max" if c in ["IdLandkreis", "Datenstand"] else "sum"
        for c in LK.columns
        if c not in key_list_BL_fix
    }
    BL = LK.groupby(by=key_list_BL_fix, as_index=False, observed=True).agg(agg_key)
    agg_key = {
        c: "max" if c in ["IdBundesland", "IdLandkreis", "Datenstand"] else "sum"
        for c in BL.columns
        if c not in key_list_ID0_fix
    }
    ID0 = BL.groupby(by=key_list_ID0_fix, as_index=False, observed=True).agg(agg_key)
    LK.drop(["IdStaat", "IdBundesland"], inplace=True, axis=1)
    BL.drop(["IdStaat", "IdLandkreis"], inplace=True, axis=1)
    ID0.drop(["IdStaat", "IdLandkreis"], inplace=True, axis=1)
    ID0["IdBundesland"] = "00"
    BL = pd.concat([ID0, BL])
    BL.reset_index(inplace=True, drop=True)
    LK_BV_valid = BV[((BV["Altersgruppe"] == "A00+") & (BV["GueltigAb"] <= Datenstand) & (BV["GueltigBis"] >= Datenstand) & (BV["AGS"].str.len() == 5))].copy()
    LK_BV_valid.reset_index(inplace=True, drop=True)
    LK_BV_valid.drop(["Altersgruppe", "GueltigAb", "GueltigBis", "männlich", "weiblich"], inplace=True, axis=1)
    LK = LK.merge(LK_BV_valid, how="right", left_on="IdLandkreis", right_on="AGS")
    LK["AnzahlFall_7d"] = np.where(LK["AnzahlFall_7d"].isnull(), 0, LK["AnzahlFall_7d"]).astype(int)
    LK["Datenstand"] = np.where(LK["Datenstand"].isnull(), Datenstand.date(), LK["Datenstand"])
    LK["incidence_7d"] = LK["AnzahlFall_7d"] / LK["Einwohner"] * 100000
    LK.drop(["Einwohner", "IdLandkreis"], inplace=True, axis=1)
    LK.rename(columns={"AGS": "IdLandkreis", "Name": "Landkreis"}, inplace=True)
    BL_BV_valid = BV[((BV["Altersgruppe"] == "A00+") & (BV["GueltigAb"] <= Datenstand) & (BV["GueltigBis"] >= Datenstand) & (BV["AGS"].str.len() == 2))].copy()
    BL_BV_valid.drop(["Altersgruppe", "GueltigAb", "GueltigBis", "männlich", "weiblich"], inplace=True, axis=1)
    BL_BV_valid.reset_index(inplace=True, drop=True)
    BL = BL.merge(BL_BV_valid, how="right", left_on="IdBundesland", right_on="AGS")
    BL["AnzahlFall_7d"] = np.where(BL["AnzahlFall_7d"].isnull(), 0, BL["AnzahlFall_7d"]).astype(int)
    BL["Datenstand"] = np.where(BL["Datenstand"].isnull(), Datenstand.date(), BL["Datenstand"])
    BL["incidence_7d"] = BL["AnzahlFall_7d"] / BL["Einwohner"] * 100000
    BL.drop(["Einwohner", "IdBundesland"], inplace=True, axis=1)
    BL.rename(columns={"AGS": "IdBundesland", "Name": "Bundesland"}, inplace=True)
    
    # store csv files, i need this csv files for personal reasons! they are not nessasary for the api!
    path = os.path.join(base_path, "..", "dataStore", "frozen-incidence")
    archivPath = os.path.join(base_path, "..", "archiv", "frozen-incidence")
    path_csv = os.path.join(base_path, "..", "archiv", "frozen-incidence", "csv")
    LK_csv_path = os.path.join(path_csv, "frozen-incidence_" + Datenstand.date().strftime("%Y-%m-%d") + "_LK.csv")
    LK.to_csv(LK_csv_path, index=False, header=True, lineterminator="\n", encoding="utf-8", date_format="%Y-%m-%d", columns=LK_dtypes_single_files.keys())
    
    LK.drop(["AnzahlFall_7d"], inplace=True, axis=1)
    BL.drop(["AnzahlFall_7d"], inplace=True, axis=1)
    # open existing kum files
    path = os.path.join(base_path, "..", "dataStore", "frozen-incidence")
    key_list_LK = ["Datenstand", "IdLandkreis"]
    key_list_BL = ["Datenstand", "IdBundesland"]
    key_list_kum = ["D", "I"]
    Datenstand2 = Datenstand.date()
    LK["Datenstand"] = pd.to_datetime(LK["Datenstand"]).dt.date
    BL["Datenstand"] = pd.to_datetime(BL["Datenstand"]).dt.date
    if (Datenstand.date().strftime("%Y-%m-%d") >="2023-04-18"):
        try:
            LK_kum_old = ut.read_json(fn="LK.json", dtype=LK_dtypes, path=path)
            BL_kum_old = ut.read_json(fn="BL.json", dtype=BL_dtypes, path=path)
            LK_kum_old["Datenstand"] = pd.to_datetime(LK_kum_old["Datenstand"]).dt.date
            BL_kum_old["Datenstand"] = pd.to_datetime(BL_kum_old["Datenstand"]).dt.date
            LK_kum_old = LK_kum_old[LK_kum_old["Datenstand"] != Datenstand2]
            BL_kum_old = BL_kum_old[BL_kum_old["Datenstand"] != Datenstand2]
            LK_kum_old = pd.concat([LK_kum_old, LK])
            LK_kum_old.sort_values(by=key_list_LK, inplace=True)
            BL_kum_old = pd.concat([BL_kum_old, BL])
            BL_kum_old.sort_values(by=key_list_BL, inplace=True)
            LK_kum_old["Datenstand"] = LK_kum_old["Datenstand"].astype(str)
            BL_kum_old["Datenstand"] = BL_kum_old["Datenstand"].astype(str)
        except:
            LK_kum_old = LK.copy()
            LK_kum_old.sort_values(by=key_list_LK, inplace=True)
            BL_kum_old = BL.copy()
            BL_kum_old.sort_values(by=key_list_BL, inplace=True)
            LK_kum_old["Datenstand"] = LK_kum_old["Datenstand"].astype(str)
            BL_kum_old["Datenstand"] = BL_kum_old["Datenstand"].astype(str)
        ut.write_json(LK_kum_old, "LK.json", path, oldDatenstandStr, archivPath)
        ut.write_json(BL_kum_old, "BL.json", path, oldDatenstandStr, archivPath)
    LK_kum_new = ut.read_json(fn="LK_complete.json", dtype=kum_dtypes, path=path)
    BL_kum_new = ut.read_json(fn="BL_complete.json", dtype=kum_dtypes, path=path)
    LK_kum_new["D"] = pd.to_datetime(LK_kum_new["D"]).dt.date
    BL_kum_new["D"] = pd.to_datetime(BL_kum_new["D"]).dt.date
    LK_kum_new = LK_kum_new[LK_kum_new["D"] != Datenstand2]
    BL_kum_new = BL_kum_new[BL_kum_new["D"] != Datenstand2]
    LK.rename(columns={"Datenstand": "D", "IdLandkreis": "I", "Landkreis": "T", "incidence_7d": "i"}, inplace=True)
    BL.rename(columns={"Datenstand": "D", "IdBundesland": "I", "Bundesland": "T", "incidence_7d": "i"}, inplace=True)
    LK_kum_new = pd.concat([LK_kum_new, LK])
    LK_kum_new.sort_values(by=key_list_kum, inplace=True)
    BL_kum_new = pd.concat([BL_kum_new, BL])
    BL_kum_new.sort_values(by=key_list_kum, inplace=True)
    LK_kum_new["D"] = LK_kum_new["D"].astype(str)
    BL_kum_new["D"] = BL_kum_new["D"].astype(str)
    ut.write_json(LK_kum_new, "LK_complete.json", path, oldDatenstandStr, archivPath)
    ut.write_json(BL_kum_new, "BL_complete.json", path, oldDatenstandStr, archivPath)
    endTime = dt.datetime.now()
    aktuelleZeit = endTime.strftime(format="%Y-%m-%dT%H:%M:%SZ")
    print(aktuelleZeit, ": total time:", endTime - startTime)
