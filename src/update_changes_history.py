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
    filename_actualmeta = "meta.json"
    filename_newmeta = "meta_new.json"
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
    full_newmeta_path = os.path.normpath(os.path.join(meta_path, filename_newmeta))
    with open(full_newmeta_path, "r", encoding="utf8") as file:
        metaObj = json.load(file)
    fileNameOrig = metaObj["filename"]
    fileSize = int(metaObj["size"])
    url = metaObj["url"]
    timeStamp = metaObj["modified"]
    Datenstand = dt.datetime.fromtimestamp(timeStamp / 1000)
    Datenstand = Datenstand.replace(hour=0, minute=0, second=0, microsecond=0)
    try:
        full_actualmeta_path = os.path.normpath(os.path.join(meta_path, filename_actualmeta))
        with open(full_actualmeta_path, "r", encoding="utf8") as file:
            metaActual = json.load(file)
        #oldDatenstandStr = dt.datetime.strptime(dt.datetime.fromtimestamp(metaActual["timestamp"] / 1000), "%Y-%m-%d")
    except:
        oldDatenstand = Datenstand.date() - dt.timedelta(days=1)
        #oldDatenstandStr = dt.datetime.strftime(oldDatenstand, "%Y-%m-%d")
    fileSizeMb = round(fileSize / 1024 / 1024, 1)
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    print(aktuelleZeit, ": loading", fileNameOrig, "(size:", fileSize, "bytes =", fileSizeMb,
          "MegaByte)")
    
    LK = ut.read_file(fn=url)
    
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    print(aktuelleZeit, ":",LK.shape[0],"rows")

    # History
    # DistrictCasesHistory, DistrictDeathsHistory, DistrictRecoveredHistory
    # StateCasesHistory, StateDeathsHistory, StateRecoveredHistory
    print(aktuelleZeit, ": calculating history data ...")
    LK.drop(["IdStaat"], inplace=True, axis=1)
    # used keylists
    key_list_LK_hist = ["IdLandkreis", "Meldedatum"]
    key_list_BL_hist = ["IdBundesland", "Meldedatum"]
    key_list_ID0_hist = ["Meldedatum"]
    LK["AnzahlFall"] = np.where(LK["NeuerFall"].isin([1, 0]), LK["AnzahlFall"], 0).astype(int)
    LK["AnzahlTodesfall"] = np.where(LK["NeuerTodesfall"].isin([1, 0, -9]), LK["AnzahlTodesfall"], 0).astype(int)
    LK["AnzahlGenesen"] = np.where(LK["NeuGenesen"].isin([1, 0, -9]), LK["AnzahlGenesen"], 0).astype(int)
    LK.drop(["NeuerFall", "NeuerTodesfall", "NeuGenesen", "Altersgruppe", "Geschlecht", "Datenstand", "Bundesland", "Landkreis"], inplace=True, axis=1)
    LK.rename(columns={"AnzahlFall": "cases", "AnzahlTodesfall": "deaths", "AnzahlGenesen": "recovered"}, inplace=True)
    agg_key = {
        c: "max"
        if c in ["IdBundesland", "Datenstand"] else "sum"
        for c in LK.columns
        if c not in key_list_LK_hist
    }
    LK = LK.groupby(by=key_list_LK_hist, as_index=False, observed=True).agg(agg_key)
    agg_key = {
        c: "max"
        if c in ["IdLandkreis", "Datenstand"] else "sum"
        for c in LK.columns
        if c not in key_list_BL_hist
    }
    BL = LK.groupby(by=key_list_BL_hist, as_index=False, observed=True).agg(agg_key)
    agg_key = {
        c: "max"
        if c in ["IdBundesland", "IdLandkreis", "Datenstand"] else "sum"
        for c in BL.columns
        if c not in key_list_ID0_hist
    }
    ID0 = BL.groupby(by=key_list_ID0_hist, as_index=False, observed=True).agg(agg_key)
    LK.drop(["IdBundesland"], inplace=True, axis=1)
    LK.sort_values(by=key_list_LK_hist, inplace=True)
    LK.reset_index(inplace=True, drop=True)
    BL.drop(["IdLandkreis"], inplace=True, axis=1)
    ID0.drop(["IdLandkreis"], inplace=True, axis=1)
    ID0["IdBundesland"] = "00"
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
    allDates = pd.DataFrame(date_range_str, columns=["Meldedatum"])
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
    BL["cases"] = BL["cases"].fillna(0).astype(int)
    BL["deaths"] = BL["deaths"].fillna(0).astype(int)
    BL["recovered"] = BL["recovered"].fillna(0).astype(int)
    
    LK["cases"] = LK["cases"].fillna(0).astype(int)
    LK["deaths"] = LK["deaths"].fillna(0).astype(int)
    LK["recovered"] = LK["recovered"].fillna(0).astype(int)
    
    BL["Meldedatum"] = BL["Meldedatum"].astype(str)
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    print(f"{aktuelleZeit} :   |-calculating BL incidence ... {BL.shape[0]} rows.")
    t1 = time.time()
    # multiprozessing BL incidence is slower! => normal apply
    BL = BL.groupby(["IdBundesland"], observed=True).apply(ut.calc_incidence) 
    t2 = time.time()
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    LKuniqueIdsCount = pd.unique(LK["IdLandkreis"]).shape[0]
    LKEstimateTime = (t2-t1) * LKuniqueIdsCount / 17
    print(f"{aktuelleZeit} :   |-Done in {round(t2-t1, 5)} sec. Estimate {round(LKEstimateTime, 5)} sec. for LK!")
    BL.reset_index(inplace=True, drop=True)
    BL.drop(["Einwohner"], inplace=True, axis=1)
        
    LK["Meldedatum"] = LK["Meldedatum"].astype(str)
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    print(f"{aktuelleZeit} :   |-calculating LK incidence ... {LK.shape[0]} rows")
    t1 = time.time()
    LK = LK.groupby(["IdLandkreis"], observed=True).apply_parallel(ut.calc_incidence, progressbar=False)
    t2 = time.time()
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    print(f"{aktuelleZeit} :   |-Done in {round(t2-t1, 5)} sec. Thats {round(LKEstimateTime/(t2 - t1), 2)} times faster")
    LK.reset_index(inplace=True, drop=True)
    LK.drop(["Einwohner"], inplace=True, axis=1)
        
    # store
    path = os.path.join(base_path, "..", "dataStore", "history")
    archivPath = os.path.join(base_path, "..", "archiv", "history")
    LKcasesHistoryFeatherFileName = "districts_cases.feather"
    LKdeathsHistoryFeatherFileName = "districts_deaths.feather"
    LKrecoveredHistoryFeatherFileName = "districts_recovered.feather"
    LKincidenceHistoryFeatherFileName = "districts_incidence.feather"
    BLcasesHistoryFeatherFileName = "states_cases.feather"
    BLdeathsHistoryFeatherFileName = "states_deaths.feather"
    BLrecoveredHistoryFeatherFileName = "states_recovered.feather"
    BLincidenceHistoryFeatherFileName = "states_incidence.feather"
    LKcasesHistoryFeatherFullPath = os.path.join(base_path, "..", "dataStore", "history", LKcasesHistoryFeatherFileName)
    LKdeathsHistoryFeatherFullPath = os.path.join(base_path, "..", "dataStore", "history", LKdeathsHistoryFeatherFileName)
    LKrecoveredHistoryFeatherFullPath = os.path.join(base_path, "..", "dataStore", "history", LKrecoveredHistoryFeatherFileName)
    LKincidenceHistoryFeatherFullPath = os.path.join(base_path, "..", "dataStore", "history", LKincidenceHistoryFeatherFileName)
    BLcasesHistoryFeatherFullPath = os.path.join(base_path, "..", "dataStore", "history", BLcasesHistoryFeatherFileName)
    BLdeathsHistoryFeatherFullPath = os.path.join(base_path, "..", "dataStore", "history", BLdeathsHistoryFeatherFileName)
    BLrecoveredHistoryFeatherFullPath = os.path.join(base_path, "..", "dataStore", "history", BLrecoveredHistoryFeatherFileName)
    BLincidenceHistoryFeatherFullPath = os.path.join(base_path, "..", "dataStore", "history", BLincidenceHistoryFeatherFileName)
    # complete districts (cases, deaths, recovered. incidence)
    #ut.write_json(LK, "districts.json", path)
    # complete states (cases, deaths, recovered. incidence)
    #ut.write_json(BL, "states.json", path)
    # complete districts (cases, deaths, recovered. incidence) short
    LK.rename(columns={"IdLandkreis": "i", "Landkreis": "t", "Meldedatum": "m", "cases": "c", "deaths": "d", "recovered": "r", "cases7d": "c7", "incidence7d": "i7"}, inplace=True)
    #ut.write_json(LK, "districts_new.json", path)
    # complete states (cases, deaths, recovered. incidence) short
    BL.rename(columns={"IdBundesland": "i", "Bundesland": "t", "Meldedatum": "m", "cases": "c", "deaths": "d", "recovered": "r", "cases7d": "c7", "incidence7d": "i7"}, inplace=True)
    #ut.write_json(BL, "states_new.json", path)
    # calculate cases diff
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    print(aktuelleZeit, ": calculating history difference")
    
    LKcases = LK.copy()
    LKcases.drop(["t", "d", "r", "c7", "i7"], inplace=True, axis=1)
    LKdeaths = LK.copy()
    LKdeaths.drop(["t", "c", "r", "c7", "i7"], inplace=True, axis=1)
    LKrecovered = LK.copy()
    LKrecovered.drop(["t", "c", "d", "c7", "i7"], inplace=True, axis=1)
    LKincidence = LK.copy()
    LKincidence.drop(["t", "c", "d", "r"], inplace=True, axis=1)
    
    BLcases = BL.copy()
    BLcases.drop(["t", "d", "r", "c7", "i7"], inplace=True, axis=1)
    BLdeaths = BL.copy()
    BLdeaths.drop(["t", "c", "r", "c7", "i7"], inplace=True, axis=1)
    BLrecovered = BL.copy()
    BLrecovered.drop(["t", "c", "d", "c7", "i7"], inplace=True, axis=1)
    BLincidence = BL.copy()
    BLincidence.drop(["t", "c", "d", "r"], inplace=True, axis=1)
    
    if os.path.exists(LKcasesHistoryFeatherFullPath):
        oldLKcases = ut.read_file(fn=LKcasesHistoryFeatherFullPath)
    ut.write_file(df=LKcases, fn=LKcasesHistoryFeatherFullPath, compression="lz4")
    if os.path.exists(LKdeathsHistoryFeatherFullPath):
        oldLKdeaths = ut.read_file(fn=LKdeathsHistoryFeatherFullPath)
    ut.write_file(df=LKdeaths, fn=LKdeathsHistoryFeatherFullPath, compression="lz4")
    if os.path.exists(LKrecoveredHistoryFeatherFullPath):
        oldLKrecovered = ut.read_file(fn=LKrecoveredHistoryFeatherFullPath)
    ut.write_file(df=LKrecovered, fn=LKrecoveredHistoryFeatherFullPath, compression="lz4")
    if os.path.exists(LKincidenceHistoryFeatherFullPath):
        oldLKincidence = ut.read_file(fn=LKincidenceHistoryFeatherFullPath)
    ut.write_file(df=LKincidence, fn=LKincidenceHistoryFeatherFullPath, compression="lz4")
    
    if os.path.exists(BLcasesHistoryFeatherFullPath):
        oldBLcases = ut.read_file(fn=BLcasesHistoryFeatherFullPath)
    ut.write_file(df=BLcases, fn=BLcasesHistoryFeatherFullPath, compression="lz4")
    if os.path.exists(BLdeathsHistoryFeatherFullPath):
        oldBLdeaths = ut.read_file(fn=BLdeathsHistoryFeatherFullPath)
    ut.write_file(df=BLdeaths, fn=BLdeathsHistoryFeatherFullPath, compression="lz4")
    if os.path.exists(BLrecoveredHistoryFeatherFullPath):
        oldBLrecovered = ut.read_file(fn=BLrecoveredHistoryFeatherFullPath)
    ut.write_file(df=BLrecovered, fn=BLrecoveredHistoryFeatherFullPath, compression="lz4")
    if os.path.exists(BLincidenceHistoryFeatherFullPath):
        oldBLincidence = ut.read_file(fn=BLincidenceHistoryFeatherFullPath)
    ut.write_file(df=BLincidence, fn=BLincidenceHistoryFeatherFullPath, compression="lz4")

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
    
    LKDiffCases["cD"] = dt.datetime.strftime(Datenstand, "%Y-%m-%d")
    LKDiffDeaths["cD"] = dt.datetime.strftime(Datenstand, "%Y-%m-%d")
    LKDiffRecovered["cD"] = dt.datetime.strftime(Datenstand, "%Y-%m-%d")
    LKDiffIncidence["cD"] = dt.datetime.strftime(Datenstand, "%Y-%m-%d")
    
    BLDiffCases["cD"] = dt.datetime.strftime(Datenstand, "%Y-%m-%d")
    BLDiffDeaths["cD"] = dt.datetime.strftime(Datenstand, "%Y-%m-%d")
    BLDiffRecovered["cD"] = dt.datetime.strftime(Datenstand, "%Y-%m-%d")
    BLDiffIncidence["cD"] = dt.datetime.strftime(Datenstand, "%Y-%m-%d")

    LKDiffCasesFullFeatherPath = os.path.join(base_path, "..", "dataStore", "history", "districts_cases_Diff.feather")
    LKDiffDeathsFullFeatherPath = os.path.join(base_path, "..", "dataStore", "history", "districts_deaths_Diff.feather")
    LKDiffRecoveredFullFeatherPath = os.path.join(base_path, "..", "dataStore", "history", "districts_recovered_Diff.feather")
    LKDiffIncidenceFullFeatherPath = os.path.join(base_path, "..", "dataStore", "history", "districts_incidence_Diff.feather")

    BLDiffCasesFullFeatherPath = os.path.join(base_path, "..", "dataStore", "history", "states_cases_Diff.feather")
    BLDiffDeathsFullFeatherPath = os.path.join(base_path, "..", "dataStore", "history", "states_deaths_Diff.feather")
    BLDiffRecoveredFullFeatherPath = os.path.join(base_path, "..", "dataStore", "history", "states_recovered_Diff.feather")
    BLDiffIncidenceFullFeatherPath = os.path.join(base_path, "..", "dataStore", "history", "states_incidence_Diff.feather")
    
    path = os.path.join(base_path, "..", "dataStore", "history")
    
    if os.path.exists(LKDiffCasesFullFeatherPath):
        LKoldDiffCases = ut.read_file(LKDiffCasesFullFeatherPath)
        LKDiffCases = pd.concat([LKoldDiffCases, LKDiffCases])
        LKDiffCases.sort_values(by=["i", "m", "cD"], inplace=True)
        LKDiffCases.reset_index(inplace=True, drop=True)
    ut.write_file(LKDiffCases, LKDiffCasesFullFeatherPath, compression="lz4")
    ut.write_json(LKDiffCases, "districts_cases_Diff.json", path)
    
    if os.path.exists(LKDiffDeathsFullFeatherPath):
        LKoldDiffDeaths = ut.read_file(LKDiffDeathsFullFeatherPath)
        LKDiffDeaths = pd.concat([LKoldDiffDeaths, LKDiffDeaths])
        LKDiffDeaths.sort_values(by=["i", "m", "cD"], inplace=True)
        LKDiffDeaths.reset_index(inplace=True, drop=True)
    ut.write_file(LKDiffDeaths, LKDiffDeathsFullFeatherPath, compression="lz4")
    ut.write_json(LKDiffDeaths, "districts_deaths_Diff.json", path)
    
    if os.path.exists(LKDiffRecoveredFullFeatherPath):
        LKoldDiffRecovered = ut.read_file(LKDiffRecoveredFullFeatherPath)
        LKDiffRecovered = pd.concat([LKoldDiffRecovered, LKDiffRecovered])
        LKDiffRecovered.sort_values(by=["i", "m", "cD"], inplace=True)
        LKDiffRecovered.reset_index(inplace=True, drop=True)
    ut.write_file(LKDiffRecovered, LKDiffRecoveredFullFeatherPath, compression="lz4")
    ut.write_json(LKDiffRecovered, "districts_recovered_Diff.json", path)
    
    if os.path.exists(LKDiffIncidenceFullFeatherPath):
        LKoldDiffIncidence = ut.read_file(LKDiffIncidenceFullFeatherPath)
        LKDiffIncidence = pd.concat([LKoldDiffIncidence, LKDiffIncidence])
        LKDiffIncidence.sort_values(by=["i", "m", "cD"], inplace=True)
        LKDiffIncidence.reset_index(inplace=True, drop=True)
    ut.write_file(LKDiffIncidence, LKDiffIncidenceFullFeatherPath, compression="lz4")
    ut.write_json(LKDiffIncidence, "districts_incidence_Diff.json", path)

    if os.path.exists(BLDiffCasesFullFeatherPath):
        BLoldDiffCases = ut.read_file(BLDiffCasesFullFeatherPath)
        BLDiffCases = pd.concat([BLoldDiffCases, BLDiffCases])
        BLDiffCases.sort_values(by=["i", "m", "cD"], inplace=True)
        BLDiffCases.reset_index(inplace=True, drop=True)
    ut.write_file(BLDiffCases, BLDiffCasesFullFeatherPath, compression="lz4")
    ut.write_json(BLDiffCases, "states_cases_Diff.json", path)
    
    if os.path.exists(BLDiffDeathsFullFeatherPath):
        BLoldDiffDeaths = ut.read_file(BLDiffDeathsFullFeatherPath)
        BLDiffDeaths = pd.concat([BLoldDiffDeaths, BLDiffDeaths])
        BLDiffDeaths.sort_values(by=["i", "m", "cD"], inplace=True)
        BLDiffDeaths.reset_index(inplace=True, drop=True)
    ut.write_file(BLDiffDeaths, BLDiffDeathsFullFeatherPath, compression="lz4")
    ut.write_json(BLDiffDeaths, "states_deaths_Diff.json", path)
    
    if os.path.exists(BLDiffRecoveredFullFeatherPath):
        BLoldDiffRecovered = ut.read_file(BLDiffRecoveredFullFeatherPath)
        BLDiffRecovered = pd.concat([BLoldDiffRecovered, BLDiffRecovered])
        BLDiffRecovered.sort_values(by=["i", "m", "cD"], inplace=True)
        BLDiffRecovered.reset_index(inplace=True, drop=True)
    ut.write_file(BLDiffRecovered, BLDiffRecoveredFullFeatherPath, compression="lz4")
    ut.write_json(BLDiffRecovered, "states_recovered_Diff.json", path)
    
    if os.path.exists(BLDiffIncidenceFullFeatherPath):
        BLoldDiffIncidence = ut.read_file(BLDiffIncidenceFullFeatherPath)
        BLDiffIncidence = pd.concat([BLoldDiffIncidence, BLDiffIncidence])
        BLDiffIncidence.sort_values(by=["i", "m", "cD"], inplace=True)
        BLDiffIncidence.reset_index(inplace=True, drop=True)
    ut.write_file(BLDiffIncidence, BLDiffIncidenceFullFeatherPath, compression="lz4")
    ut.write_json(BLDiffIncidence, "states_incidence_Diff.json", path)

