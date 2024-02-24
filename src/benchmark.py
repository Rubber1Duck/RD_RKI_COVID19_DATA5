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
    
    print(f"{aktuelleZeit} :   |-calculating BL incidence with single thread... {BL.shape[0]} rows.")
    BLS = BL.copy()
    BLS["ID"] = BLS["IdBundesland"]
    t1 = time.time()
    BLS = BLS.groupby(["ID"]).apply(ut.calc_incidence, include_groups=False)
    t2 = time.time()
    singleTime = t2 - t1
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    LKuniqueIdsCount = pd.unique(LK["IdLandkreis"]).shape[0]
    LKEstimateTime = singleTime * LKuniqueIdsCount / 17
    print(f"{aktuelleZeit} :   |-Done in {round(singleTime, 5)} sec. Estimate {round(LKEstimateTime, 5)} sec. for LK!")
    print(f"{aktuelleZeit} :   |-calculating BL incidence with 4 Processes... {BL.shape[0]} rows.")
    t1 = time.time()
    BL = BL.groupby(["IdBundesland"], observed=True).apply_parallel(ut.calc_incidence, progressbar=False)
    t2 = time.time()
    multiTime = t2 - t1
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    print(f"{aktuelleZeit} :   |-Done in {round(t2 - t1, 5)} sec. {round(singleTime / multiTime, 5)} times faster as single thread.")
    BL.reset_index(inplace=True, drop=True)
    BL.drop(["Einwohner"], inplace=True, axis=1)
        
    LK["Meldedatum"] = LK["Meldedatum"].astype(str)
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    print(f"{aktuelleZeit} :   |-calculating LK incidence with {os.cpu_count()} processes... {LK.shape[0]} rows")
    t1 = time.time()
    LK = LK.groupby(["IdLandkreis"], observed=True).apply_parallel(ut.calc_incidence, progressbar=False)
    t2 = time.time()
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    print(f"{aktuelleZeit} :   |-Done in {round(t2-t1, 5)} sec. Thats {round(LKEstimateTime/(t2 - t1), 2)} times faster as single thread estimated!")
    LK.reset_index(inplace=True, drop=True)
    LK.drop(["Einwohner"], inplace=True, axis=1)

    '''    
    # store all files not compressed! will be done later
    changesPath = os.path.normpath(os.path.join(base_path, "..", "dataStore", "historychanges"))
    LKFeatherFile = "districts.feather"
    BLFeatherFile = "states.feather"
    LKcasesChangesFeatherFull = os.path.join(changesPath, "cases", LKFeatherFile)
    LKdeathsChangesFeatherFull = os.path.join(changesPath, "deaths", LKFeatherFile)
    LKrecoveredChangesFeatherFull = os.path.join(changesPath, "recovered", LKFeatherFile)
    LKincidenceChangesFeatherFull = os.path.join(changesPath, "incidence", LKFeatherFile)
    BLcasesChangesFeatherFull = os.path.join(changesPath, "cases", BLFeatherFile)
    BLdeathsChangesFeatherFull = os.path.join(changesPath, "deaths", BLFeatherFile)
    BLrecoveredChangesFeatherFull = os.path.join(changesPath, "recovered", BLFeatherFile)
    BLincidenceChangesFeatherFull = os.path.join(changesPath, "incidence", BLFeatherFile)
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
    
    if os.path.exists(LKcasesChangesFeatherFull):
        oldLKcases = ut.read_file(LKcasesChangesFeatherFull)
    ut.write_file(LKcases, LKcasesChangesFeatherFull, compression="lz4")
    if os.path.exists(LKdeathsChangesFeatherFull):
        oldLKdeaths = ut.read_file(LKdeathsChangesFeatherFull)
    ut.write_file(LKdeaths, LKdeathsChangesFeatherFull, compression="lz4")
    if os.path.exists(LKrecoveredChangesFeatherFull):
        oldLKrecovered = ut.read_file(LKrecoveredChangesFeatherFull)
    ut.write_file(LKrecovered, LKrecoveredChangesFeatherFull, compression="lz4")
    if os.path.exists(LKincidenceChangesFeatherFull):
        oldLKincidence = ut.read_file(LKincidenceChangesFeatherFull)
    ut.write_file(LKincidence, LKincidenceChangesFeatherFull, compression="lz4")
    
    if os.path.exists(BLcasesChangesFeatherFull):
        oldBLcases = ut.read_file(BLcasesChangesFeatherFull)
    ut.write_file(BLcases, BLcasesChangesFeatherFull, compression="lz4")
    if os.path.exists(BLdeathsChangesFeatherFull):
        oldBLdeaths = ut.read_file(BLdeathsChangesFeatherFull)
    ut.write_file(BLdeaths, BLdeathsChangesFeatherFull, compression="lz4")
    if os.path.exists(BLrecoveredChangesFeatherFull):
        oldBLrecovered = ut.read_file(BLrecoveredChangesFeatherFull)
    ut.write_file(BLrecovered, BLrecoveredChangesFeatherFull, compression="lz4")
    if os.path.exists(BLincidenceChangesFeatherFull):
        oldBLincidence = ut.read_file(BLincidenceChangesFeatherFull)
    ut.write_file(BLincidence, BLincidenceChangesFeatherFull, compression="lz4")

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
    
    path = os.path.normpath(os.path.join(base_path, "..", "dataStore", "historychanges", "cases"))
    if os.path.exists(LKDiffCasesFeatherFull):
        LKoldDiffCases = ut.read_file(LKDiffCasesFeatherFull)
        LKDiffCases = pd.concat([LKoldDiffCases, LKDiffCases])
    LKDiffCases.sort_values(by=["i", "m", "cD"], inplace=True)
    LKDiffCases.reset_index(inplace=True, drop=True)
    ut.write_file(LKDiffCases, LKDiffCasesFeatherFull, compression="lz4")
    ut.write_json(LKDiffCases, "districts_Diff.json", path)
    
    path = os.path.normpath(os.path.join(base_path, "..", "dataStore", "historychanges", "deaths"))
    if os.path.exists(LKDiffDeathsFeatherFull):
        LKoldDiffDeaths = ut.read_file(LKDiffDeathsFeatherFull)
        LKDiffDeaths = pd.concat([LKoldDiffDeaths, LKDiffDeaths])
    LKDiffDeaths.sort_values(by=["i", "m", "cD"], inplace=True)
    LKDiffDeaths.reset_index(inplace=True, drop=True)
    ut.write_file(LKDiffDeaths, LKDiffDeathsFeatherFull, compression="lz4")
    ut.write_json(LKDiffDeaths, "districts_Diff.json", path)
    
    path = os.path.normpath(os.path.join(base_path, "..", "dataStore", "historychanges", "recovered"))
    if os.path.exists(LKDiffRecoveredFeatherFull):
        LKoldDiffRecovered = ut.read_file(LKDiffRecoveredFeatherFull)
        LKDiffRecovered = pd.concat([LKoldDiffRecovered, LKDiffRecovered])
    LKDiffRecovered.sort_values(by=["i", "m", "cD"], inplace=True)
    LKDiffRecovered.reset_index(inplace=True, drop=True)
    ut.write_file(LKDiffRecovered, LKDiffRecoveredFeatherFull, compression="lz4")
    ut.write_json(LKDiffRecovered, "districts_Diff.json", path)
    
    path = os.path.normpath(os.path.join(base_path, "..", "dataStore", "historychanges", "incidence"))
    if os.path.exists(LKDiffIncidenceFeatherFull):
        LKoldDiffIncidence = ut.read_file(LKDiffIncidenceFeatherFull)
        LKDiffIncidence = pd.concat([LKoldDiffIncidence, LKDiffIncidence])
    LKDiffIncidence.sort_values(by=["i", "m", "cD"], inplace=True)
    LKDiffIncidence.reset_index(inplace=True, drop=True)
    ut.write_file(LKDiffIncidence, LKDiffIncidenceFeatherFull, compression="lz4")
    ut.write_json(LKDiffIncidence, "districts_Diff.json", path)

    path = os.path.normpath(os.path.join(base_path, "..", "dataStore", "historychanges", "cases"))
    if os.path.exists(BLDiffCasesFeatherFull):
        BLoldDiffCases = ut.read_file(BLDiffCasesFeatherFull)
        BLDiffCases = pd.concat([BLoldDiffCases, BLDiffCases])
    BLDiffCases.sort_values(by=["i", "m", "cD"], inplace=True)
    BLDiffCases.reset_index(inplace=True, drop=True)
    ut.write_file(BLDiffCases, BLDiffCasesFeatherFull, compression="lz4")
    ut.write_json(BLDiffCases, "states_Diff.json", path)
    
    path = os.path.normpath(os.path.join(base_path, "..", "dataStore", "historychanges", "deaths"))
    if os.path.exists(BLDiffDeathsFeatherFull):
        BLoldDiffDeaths = ut.read_file(BLDiffDeathsFeatherFull)
        BLDiffDeaths = pd.concat([BLoldDiffDeaths, BLDiffDeaths])
    BLDiffDeaths.sort_values(by=["i", "m", "cD"], inplace=True)
    BLDiffDeaths.reset_index(inplace=True, drop=True)
    ut.write_file(BLDiffDeaths, BLDiffDeathsFeatherFull, compression="lz4")
    ut.write_json(BLDiffDeaths, "states_Diff.json", path)
    
    path = os.path.normpath(os.path.join(base_path, "..", "dataStore", "historychanges", "recovered"))
    if os.path.exists(BLDiffRecoveredFeatherFull):
        BLoldDiffRecovered = ut.read_file(BLDiffRecoveredFeatherFull)
        BLDiffRecovered = pd.concat([BLoldDiffRecovered, BLDiffRecovered])
    BLDiffRecovered.sort_values(by=["i", "m", "cD"], inplace=True)
    BLDiffRecovered.reset_index(inplace=True, drop=True)
    ut.write_file(BLDiffRecovered, BLDiffRecoveredFeatherFull, compression="lz4")
    ut.write_json(BLDiffRecovered, "states_Diff.json", path)
    
    path = os.path.normpath(os.path.join(base_path, "..", "dataStore", "historychanges", "incidence"))
    if os.path.exists(BLDiffIncidenceFeatherFull):
        BLoldDiffIncidence = ut.read_file(BLDiffIncidenceFeatherFull)
        BLDiffIncidence = pd.concat([BLoldDiffIncidence, BLDiffIncidence])
    BLDiffIncidence.sort_values(by=["i", "m", "cD"], inplace=True)
    BLDiffIncidence.reset_index(inplace=True, drop=True)
    ut.write_file(BLDiffIncidence, BLDiffIncidenceFeatherFull, compression="lz4")
    ut.write_json(BLDiffIncidence, "states_Diff.json", path)
    '''