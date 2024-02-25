import os
import datetime as dt
import numpy as np
import pandas as pd
import json
import utils as ut
import gc
import time
from multiprocess_pandas import applyparallel

def update():
    base_path = os.path.dirname(os.path.abspath(__file__))
    meta_path = os.path.join(base_path, "..", "dataStore", "meta")
    meta_path = os.path.normpath(meta_path)
    file_oldmeta = "meta.json"
    file_newmeta = "meta_new.json"
    BV_csv_path = os.path.join(base_path, "..", "Bevoelkerung", "Bevoelkerung.csv")
    BV_csv_path = os.path.normpath(BV_csv_path)
    BV_dtypes = {"AGS": "str", "Altersgruppe": "str", "Name": "str", "GueltigAb": "object", "GueltigBis": "object", "Einwohner": "Int32", "männlich": "Int32", "weiblich": "Int32"}
    
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
    # load covid latest from web
    newmetaFull = os.path.normpath(os.path.join(meta_path, file_newmeta))
    with open(newmetaFull, "r", encoding="utf8") as file:
        newMetaObj = json.load(file)
    fileNameOrig = newMetaObj["filename"]
    fileSize = int(newMetaObj["size"])
    url = newMetaObj["url"]
    timeStamp = newMetaObj["modified"]
    Datenstand = dt.datetime.fromtimestamp(timeStamp / 1000)
    Datenstand = Datenstand.replace(hour=0, minute=0, second=0, microsecond=0)
    try:
        oldmetaFull = os.path.normpath(os.path.join(meta_path, file_oldmeta))
        with open(oldmetaFull, "r", encoding="utf8") as file:
            oldMetaObj = json.load(file)
        oldDatenstandStr = dt.datetime.strptime(dt.datetime.fromtimestamp(oldMetaObj["timestamp"] / 1000), "%Y-%m-%d")
    except:
        oldDatenstand = Datenstand.date() - dt.timedelta(days=1)
        oldDatenstandStr = dt.datetime.strftime(oldDatenstand, "%Y-%m-%d")
    fileSizeMb = round(fileSize / 1024 / 1024, 1)
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    print(f"{aktuelleZeit} : loading {fileNameOrig} (size: {fileSize} bytes = {fileSizeMb} MegaByte)")
    
    LK = ut.read_file(fn=url)
    
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    print(f"{aktuelleZeit} : {LK.shape[0]} rows")

    # History
    # DistrictCasesHistory, DistrictDeathsHistory, DistrictRecoveredHistory
    # StateCasesHistory, StateDeathsHistory, StateRecoveredHistory
    print(f"{aktuelleZeit} : calculating history ...")
    LK.drop(["IdStaat"], inplace=True, axis=1)
    # used keylists
    key_list_LK_hist = ["IdLandkreis", "Meldedatum"]
    key_list_BL_hist = ["IdBundesland", "Meldedatum"]
    key_list_ID0_hist = ["Meldedatum"]
    LK["AnzahlFall"] = np.where(LK["NeuerFall"].isin([1, 0]), LK["AnzahlFall"], 0).astype(int)
    LK["AnzahlTodesfall"] = np.where(LK["NeuerTodesfall"].isin([1, 0, -9]), LK["AnzahlTodesfall"], 0).astype(int)
    LK["AnzahlGenesen"] = np.where(LK["NeuGenesen"].isin([1, 0, -9]), LK["AnzahlGenesen"], 0).astype(int)
    LK.drop(["NeuerFall", "NeuerTodesfall", "NeuGenesen", "Altersgruppe", "Geschlecht", "Bundesland", "Landkreis", "Datenstand"], inplace=True, axis=1)
    LK.rename(columns={"AnzahlFall": "c", "AnzahlTodesfall": "d", "AnzahlGenesen": "r"}, inplace=True)
    agg_key = {
        c: "max"
        if c in ["IdBundesland"] else "sum"
        for c in LK.columns
        if c not in key_list_LK_hist
    }
    LK = LK.groupby(by=key_list_LK_hist, as_index=False, observed=True).agg(agg_key)
    agg_key = {
        c: "max"
        if c in ["IdLandkreis"] else "sum"
        for c in LK.columns
        if c not in key_list_BL_hist
    }
    BL = LK.groupby(by=key_list_BL_hist, as_index=False, observed=True).agg(agg_key)
    agg_key = {
        c: "max"
        if c in ["IdBundesland", "IdLandkreis"] else "sum"
        for c in BL.columns
        if c not in key_list_ID0_hist
    }
    ID0 = BL.groupby(by=key_list_ID0_hist, as_index=False, observed=True).agg(agg_key)
    LK.drop(["IdBundesland"], inplace=True, axis=1)
    LK.sort_values(by=key_list_LK_hist, inplace=True)
    LK.reset_index(inplace=True, drop=True)
    ID0["IdBundesland"] = "00"
    BL = pd.concat([ID0, BL])
    BL.drop(["IdLandkreis"], inplace=True, axis=1)
    BL.sort_values(by=key_list_BL_hist, inplace=True)
    BL.reset_index(inplace=True, drop=True)
    LK["Meldedatum"] = pd.to_datetime(LK["Meldedatum"]).dt.date
    BL["Meldedatum"] = pd.to_datetime(BL["Meldedatum"]).dt.date
    # fill dates for every region
    startDate = "2020-01-01"
    allDates = pd.DataFrame(pd.date_range(end=(Datenstand - dt.timedelta(days=1)), start=startDate), columns=["Meldedatum"])
    BLDates = pd.DataFrame(pd.unique(BL["IdBundesland"]).copy(), columns=["IdBundesland"])
    LKDates = pd.DataFrame(pd.unique(LK["IdLandkreis"]).copy(), columns=["IdLandkreis"])
    
    # add Bundesland, Landkreis and Einwohner
    BV_mask = ((BV_BL_A00["AGS"].isin(BLDates["IdBundesland"])) & (BV_BL_A00["GueltigAb"] <= Datenstand) & (BV_BL_A00["GueltigBis"] >= Datenstand))
    BV_masked = BV_BL_A00[BV_mask].copy()
    BV_masked.drop(["GueltigAb", "GueltigBis", "Altersgruppe", "männlich", "weiblich"], inplace=True, axis=1)
    BV_masked.rename(columns={"AGS": "IdBundesland", "Name": "Bundesland"}, inplace=True)
    BLDates = BLDates.merge(right=BV_masked, on=["IdBundesland"], how="left")
        
    BV_mask = ((BV_LK_A00["AGS"].isin(LKDates["IdLandkreis"])) & (BV_LK_A00["GueltigAb"] <= Datenstand) & (BV_LK_A00["GueltigBis"] >= Datenstand))
    BV_masked = BV_LK_A00[BV_mask].copy()
    BV_masked.drop(["GueltigAb", "GueltigBis", "Altersgruppe", "männlich", "weiblich"], inplace=True, axis=1)
    BV_masked.rename(columns={"AGS": "IdLandkreis", "Name": "Landkreis"}, inplace=True)
    LKDates = LKDates.merge(right=BV_masked, on="IdLandkreis", how="left")
        
    BLDates = BLDates.merge(allDates, how="cross")
    BLDates = ut.squeeze_dataframe(BLDates)
    LKDates = LKDates.merge(allDates, how="cross")
    LKDates = ut.squeeze_dataframe(LKDates)
    BLDates["Meldedatum"] = pd.to_datetime(BLDates["Meldedatum"]).dt.date
    LKDates["Meldedatum"] = pd.to_datetime(LKDates["Meldedatum"]).dt.date
    BL = BL.merge(BLDates, how="right", on=["IdBundesland", "Meldedatum"])
    LK = LK.merge(LKDates, how="right", on=["IdLandkreis", "Meldedatum"])
    
    # clear unneeded data
    ID0 = pd.DataFrame()
    allDates = pd.DataFrame()
    BLDates = pd.DataFrame()
    LKDates = pd.DataFrame()
    BV_mask = pd.DataFrame()
    BV_masked = pd.DataFrame()
    del ID0
    del allDates
    del BLDates
    del LKDates
    del BV_mask
    del BV_masked
    gc.collect()
    
    #fill nan with 0
    BL["c"] = BL["c"].fillna(0).astype(int)
    BL["d"] = BL["d"].fillna(0).astype(int)
    BL["r"] = BL["r"].fillna(0).astype(int)
    
    LK["c"] = LK["c"].fillna(0).astype(int)
    LK["d"] = LK["d"].fillna(0).astype(int)
    LK["r"] = LK["r"].fillna(0).astype(int)
    
    BL["Meldedatum"] = BL["Meldedatum"].astype(str)
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    print(f"{aktuelleZeit} :   |-calculating BL incidence with {os.cpu_count()} processes... {BL.shape[0]} rows.")
    t1 = time.time()
    BL = BL.groupby(["IdBundesland"], observed=True).apply_parallel(ut.calc_incidence, progressbar=False)
    t2 = time.time()
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    LKuniqueIdsCount = pd.unique(LK["IdLandkreis"]).shape[0]
    LKEstimateTime = (t2 - t1) * LKuniqueIdsCount / 17
    print(f"{aktuelleZeit} :   |-Done in {round(t2 - t1, 5)} sec. Estimate {round(LKEstimateTime, 5)} sec. for LK!")
    BL.reset_index(inplace=True, drop=True)
    BL.drop(["Einwohner"], inplace=True, axis=1)
        
    LK["Meldedatum"] = LK["Meldedatum"].astype(str)
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    print(f"{aktuelleZeit} :   |-calculating LK incidence with {os.cpu_count()} processes... {LK.shape[0]} rows.")
    t1 = time.time()
    LK = LK.groupby(["IdLandkreis"], observed=True).apply_parallel(ut.calc_incidence, progressbar=False)
    t2 = time.time()
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    print(f"{aktuelleZeit} :   |-Done in {round(t2-t1, 5)} sec. Thats {round(LKEstimateTime/(t2 - t1), 2)} times faster than estimated")
    LK.reset_index(inplace=True, drop=True)
    LK.drop(["Einwohner"], inplace=True, axis=1)
        
    # store all files not compressed! will be done later
    filesToConvert = []
    historyPath = os.path.normpath(os.path.join(base_path, "..", "dataStore", "history"))
    LKFeatherFile = "districts.feather"
    BLFeatherFile = "states.feather"
    LKcasesFeatherFull = os.path.join(historyPath, "cases", LKFeatherFile)
    LKdeathsFeatherFull = os.path.join(historyPath, "deaths", LKFeatherFile)
    LKrecoveredFeatherFull = os.path.join(historyPath, "recovered", LKFeatherFile)
    LKincidenceFeatherFull = os.path.join(historyPath, "incidence", LKFeatherFile)
    BLcasesFeatherFull = os.path.join(historyPath, "cases", BLFeatherFile)
    BLdeathsFeatherFull = os.path.join(historyPath, "deaths", BLFeatherFile)
    BLrecoveredFeatherFull = os.path.join(historyPath, "recovered", BLFeatherFile)
    BLincidenceFeatherFull = os.path.join(historyPath, "incidence", BLFeatherFile)
    
    # for smaler files rename fields
    # i = Id(Landkreis or Bundesland)
    # t = Name of Id(Landkreis or Bundesland)
    # m = Meldedatum
    # c = cases
    # d = deaths
    # r = recovered
    # c7 = cases7d (cases7days)
    # i7 = incidence7d (incidence7days)

    LK.rename(columns={"IdLandkreis": "i", "Landkreis": "t", "Meldedatum": "m"}, inplace=True)
    BL.rename(columns={"IdBundesland": "i", "Bundesland": "t", "Meldedatum": "m"}, inplace=True)
    
    # split LK
    LKcases = LK.copy()
    LKcases.drop(["t", "d", "r", "c7", "i7"], inplace=True, axis=1)
    LKdeaths = LK.copy()
    LKdeaths.drop(["t", "c", "r", "c7", "i7"], inplace=True, axis=1)
    LKrecovered = LK.copy()
    LKrecovered.drop(["t", "c", "d", "c7", "i7"], inplace=True, axis=1)
    LKincidence = LK.copy()
    LKincidence.drop(["t", "c", "d", "r"], inplace=True, axis=1)
    # free memory
    LK = pd.DataFrame()
    del LK
    gc.collect()

    # split BL
    BLcases = BL.copy()
    BLcases.drop(["t", "d", "r", "c7", "i7"], inplace=True, axis=1)
    BLdeaths = BL.copy()
    BLdeaths.drop(["t", "c", "r", "c7", "i7"], inplace=True, axis=1)
    BLrecovered = BL.copy()
    BLrecovered.drop(["t", "c", "d", "c7", "i7"], inplace=True, axis=1)
    BLincidence = BL.copy()
    BLincidence.drop(["t", "c", "d", "r"], inplace=True, axis=1)
    # free memory
    BL = pd.DataFrame()
    del BL
    gc.collect()

    # read oldLK(cases, deaths, recovered, incidence) if old file exist
    # write new data 
    if os.path.exists(LKcasesFeatherFull):
        oldLKcases = ut.read_file(LKcasesFeatherFull)
    ut.write_file(LKcases, LKcasesFeatherFull, compression="lz4")
    filesToConvert.append((LKcasesFeatherFull, "districts.json", os.path.join(historyPath, "cases")))
    if os.path.exists(LKdeathsFeatherFull):
        oldLKdeaths = ut.read_file(LKdeathsFeatherFull)
    ut.write_file(LKdeaths, LKdeathsFeatherFull, compression="lz4")
    filesToConvert.append((LKdeathsFeatherFull, "districts.json", os.path.join(historyPath, "deaths")))
    if os.path.exists(LKrecoveredFeatherFull):
        oldLKrecovered = ut.read_file(LKrecoveredFeatherFull)
    ut.write_file(LKrecovered, LKrecoveredFeatherFull, compression="lz4")
    filesToConvert.append((LKrecoveredFeatherFull, "districts.json", os.path.join(historyPath, "recovered")))
    if os.path.exists(LKincidenceFeatherFull):
        oldLKincidence = ut.read_file(LKincidenceFeatherFull)
    ut.write_file(LKincidence, LKincidenceFeatherFull, compression="lz4")
    filesToConvert.append((LKincidenceFeatherFull, "districts.json", os.path.join(historyPath, "incidence")))
    
    
    # read oldBL(cases, deaths, recovered, incidence) if old file exist
    # write new data
    if os.path.exists(BLcasesFeatherFull):
        oldBLcases = ut.read_file(BLcasesFeatherFull)
    ut.write_file(BLcases, BLcasesFeatherFull, compression="lz4")
    filesToConvert.append((BLcasesFeatherFull, "states.json", os.path.join(historyPath, "cases")))
    if os.path.exists(BLdeathsFeatherFull):
        oldBLdeaths = ut.read_file(BLdeathsFeatherFull)
    ut.write_file(BLdeaths, BLdeathsFeatherFull, compression="lz4")
    filesToConvert.append((BLdeathsFeatherFull, "states.json", os.path.join(historyPath, "deaths")))
    if os.path.exists(BLrecoveredFeatherFull):
        oldBLrecovered = ut.read_file(BLrecoveredFeatherFull)
    ut.write_file(BLrecovered, BLrecoveredFeatherFull, compression="lz4")
    filesToConvert.append((BLrecoveredFeatherFull, "states.json", os.path.join(historyPath, "recovered")))
    if os.path.exists(BLincidenceFeatherFull):
        oldBLincidence = ut.read_file(BLincidenceFeatherFull)
    ut.write_file(BLincidence, BLincidenceFeatherFull, compression="lz4")
    filesToConvert.append((BLincidenceFeatherFull, "states.json", os.path.join(historyPath, "incidence")))

    # calculate diff data
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    print(aktuelleZeit, ": calculating history difference")
    changesPath = os.path.normpath(os.path.join(base_path, "..", "dataStore", "historychanges"))
    try:
        LKDiffCases = ut.get_different_rows(oldLKcases, LKcases)
    except:
        LKDiffCases = LKcases.copy()
    try:
        LKDiffDeaths = ut.get_different_rows(oldLKdeaths, LKdeaths)
    except:
        LKDiffDeaths = LKdeaths.copy()
    try:
        LKDiffRecovered = ut.get_different_rows(oldLKrecovered, LKrecovered)
    except:
        LKDiffRecovered = LKrecovered.copy()
    try:
        LKDiffIncidence = ut.get_different_rows(oldLKincidence, LKincidence)
    except:
        LKDiffIncidence = LKincidence.copy()

    try:
        BLDiffCases = ut.get_different_rows(oldBLcases, BLcases)
    except:
        BLDiffCases = BLcases.copy()
    try:
        BLDiffDeaths = ut.get_different_rows(oldBLdeaths, BLdeaths)
    except:
        BLDiffDeaths = BLdeaths.copy()
    try:
        BLDiffRecovered = ut.get_different_rows(oldBLrecovered, BLrecovered)
    except:
        BLDiffRecovered = BLrecovered.copy()
    try:
        BLDiffIncidence = ut.get_different_rows(oldBLincidence, BLincidence)
    except:
        BLDiffIncidence = BLincidence.copy()
    
    ChangeDateStr = dt.datetime.strftime(Datenstand, "%Y-%m-%d")
    LKDiffCases["cD"] = ChangeDateStr
    LKDiffDeaths["cD"] = ChangeDateStr
    LKDiffRecovered["cD"] = ChangeDateStr
    LKDiffIncidence["cD"] = ChangeDateStr
    
    BLDiffCases["cD"] = ChangeDateStr
    BLDiffDeaths["cD"] = ChangeDateStr
    BLDiffRecovered["cD"] = ChangeDateStr
    BLDiffIncidence["cD"] = ChangeDateStr

    districtsDiffFeatherFile = "districts_Diff.feather"
    LKDiffCasesFeatherFull = os.path.join(changesPath, "cases", districtsDiffFeatherFile)
    LKDiffDeathsFeatherFull = os.path.join(changesPath, "deaths", districtsDiffFeatherFile)
    LKDiffRecoveredFeatherFull = os.path.join(changesPath, "recovered", districtsDiffFeatherFile)
    LKDiffIncidenceFeatherFull = os.path.join(changesPath, "incidence", districtsDiffFeatherFile)

    statesDiffFeatherFile = "states_Diff.feather"
    BLDiffCasesFeatherFull = os.path.join(changesPath, "cases", statesDiffFeatherFile)
    BLDiffDeathsFeatherFull = os.path.join(changesPath, "deaths", statesDiffFeatherFile)
    BLDiffRecoveredFeatherFull = os.path.join(changesPath, "recovered", statesDiffFeatherFile)
    BLDiffIncidenceFeatherFull = os.path.join(changesPath, "incidence", statesDiffFeatherFile)
    
    path = os.path.join(changesPath, "cases")
    if os.path.exists(LKDiffCasesFeatherFull):
        LKoldDiffCases = ut.read_file(LKDiffCasesFeatherFull)
        LKoldDiffCases = LKoldDiffCases[LKoldDiffCases["cD"] != ChangeDateStr]
        LKDiffCases = pd.concat([LKoldDiffCases, LKDiffCases])
    LKDiffCases.sort_values(by=["i", "m", "cD"], inplace=True)
    LKDiffCases.reset_index(inplace=True, drop=True)
    ut.write_file(LKDiffCases, LKDiffCasesFeatherFull, compression="lz4")
    filesToConvert.append((LKDiffCasesFeatherFull, "districts_Diff.json", path))
    
    path = os.path.join(changesPath, "deaths")
    if os.path.exists(LKDiffDeathsFeatherFull):
        LKoldDiffDeaths = ut.read_file(LKDiffDeathsFeatherFull)
        LKoldDiffDeaths = LKoldDiffDeaths[LKoldDiffDeaths["cD"] != ChangeDateStr]
        LKDiffDeaths = pd.concat([LKoldDiffDeaths, LKDiffDeaths])
    LKDiffDeaths.sort_values(by=["i", "m", "cD"], inplace=True)
    LKDiffDeaths.reset_index(inplace=True, drop=True)
    ut.write_file(LKDiffDeaths, LKDiffDeathsFeatherFull, compression="lz4")
    filesToConvert.append((LKDiffDeathsFeatherFull, "districts_Diff.json", path))
    
    path = os.path.join(changesPath, "recovered")
    if os.path.exists(LKDiffRecoveredFeatherFull):
        LKoldDiffRecovered = ut.read_file(LKDiffRecoveredFeatherFull)
        LKoldDiffRecovered = LKoldDiffRecovered[LKoldDiffRecovered["cD"] != ChangeDateStr]
        LKDiffRecovered = pd.concat([LKoldDiffRecovered, LKDiffRecovered])
    LKDiffRecovered.sort_values(by=["i", "m", "cD"], inplace=True)
    LKDiffRecovered.reset_index(inplace=True, drop=True)
    ut.write_file(LKDiffRecovered, LKDiffRecoveredFeatherFull, compression="lz4")
    filesToConvert.append((LKDiffRecoveredFeatherFull, "districts_Diff.json", path))
    
    path = os.path.join(changesPath, "incidence")
    if os.path.exists(LKDiffIncidenceFeatherFull):
        LKoldDiffIncidence = ut.read_file(LKDiffIncidenceFeatherFull)
        LKoldDiffIncidence = LKoldDiffIncidence[LKoldDiffIncidence["cD"] != ChangeDateStr]
        LKDiffIncidence = pd.concat([LKoldDiffIncidence, LKDiffIncidence])
    LKDiffIncidence.sort_values(by=["i", "m", "cD"], inplace=True)
    LKDiffIncidence.reset_index(inplace=True, drop=True)
    ut.write_file(LKDiffIncidence, LKDiffIncidenceFeatherFull, compression="lz4")
    filesToConvert.append((LKDiffIncidenceFeatherFull, "districts_Diff.json", path))

    path = os.path.normpath(os.path.join(base_path, "..", "dataStore", "historychanges", "cases"))
    if os.path.exists(BLDiffCasesFeatherFull):
        BLoldDiffCases = ut.read_file(BLDiffCasesFeatherFull)
        BLoldDiffCases = BLoldDiffCases[BLoldDiffCases["cD"] != ChangeDateStr]
        BLDiffCases = pd.concat([BLoldDiffCases, BLDiffCases])
    BLDiffCases.sort_values(by=["i", "m", "cD"], inplace=True)
    BLDiffCases.reset_index(inplace=True, drop=True)
    ut.write_file(BLDiffCases, BLDiffCasesFeatherFull, compression="lz4")
    filesToConvert.append((BLDiffCasesFeatherFull, "states_Diff.json", path))
    
    path = os.path.normpath(os.path.join(base_path, "..", "dataStore", "historychanges", "deaths"))
    if os.path.exists(BLDiffDeathsFeatherFull):
        BLoldDiffDeaths = ut.read_file(BLDiffDeathsFeatherFull)
        BLoldDiffDeaths = BLoldDiffDeaths[BLoldDiffDeaths["cD"] != ChangeDateStr]
        BLDiffDeaths = pd.concat([BLoldDiffDeaths, BLDiffDeaths])
    BLDiffDeaths.sort_values(by=["i", "m", "cD"], inplace=True)
    BLDiffDeaths.reset_index(inplace=True, drop=True)
    ut.write_file(BLDiffDeaths, BLDiffDeathsFeatherFull, compression="lz4")
    filesToConvert.append((BLDiffDeathsFeatherFull, "states_Diff.json", path))
    
    path = os.path.normpath(os.path.join(base_path, "..", "dataStore", "historychanges", "recovered"))
    if os.path.exists(BLDiffRecoveredFeatherFull):
        BLoldDiffRecovered = ut.read_file(BLDiffRecoveredFeatherFull)
        BLoldDiffRecovered = BLoldDiffRecovered[BLoldDiffRecovered["cD"] != ChangeDateStr]
        BLDiffRecovered = pd.concat([BLoldDiffRecovered, BLDiffRecovered])
    BLDiffRecovered.sort_values(by=["i", "m", "cD"], inplace=True)
    BLDiffRecovered.reset_index(inplace=True, drop=True)
    ut.write_file(BLDiffRecovered, BLDiffRecoveredFeatherFull, compression="lz4")
    filesToConvert.append((BLDiffRecoveredFeatherFull, "states_Diff.json", path))
    
    path = os.path.normpath(os.path.join(base_path, "..", "dataStore", "historychanges", "incidence"))
    if os.path.exists(BLDiffIncidenceFeatherFull):
        BLoldDiffIncidence = ut.read_file(BLDiffIncidenceFeatherFull)
        BLoldDiffIncidence = BLoldDiffIncidence[BLoldDiffIncidence["cD"] != ChangeDateStr]
        BLDiffIncidence = pd.concat([BLoldDiffIncidence, BLDiffIncidence])
    BLDiffIncidence.sort_values(by=["i", "m", "cD"], inplace=True)
    BLDiffIncidence.reset_index(inplace=True, drop=True)
    ut.write_file(BLDiffIncidence, BLDiffIncidenceFeatherFull, compression="lz4")
    filesToConvert.append((BLDiffIncidenceFeatherFull, "states_Diff.json", path))
    return filesToConvert