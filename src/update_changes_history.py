import os
import datetime as dt
import pandas as pd
import utils as ut
import gc

def update(meta):
    HC_dtp = {"i": "str", "m": "object", "c": "int64"}
    HD_dtp = {"i": "str", "m": "object", "d": "int64"}
    HR_dtp = {"i": "str", "m": "object", "r": "int64"}
    HI_dtp = {"i": "str", "m": "object", "c7": "int64", "i7": "float"}

    HCC_dtp = {"i": "str", "m": "object", "c": "int64", "dc": "int64", "cD": "object"}
    HCD_dtp = {"i": "str", "m": "object", "d": "int64", "cD": "object"}
    HCR_dtp = {"i": "str", "m": "object", "r": "int64", "cD": "object"}
    HCI_dtp = {"i": "str", "m": "object", "c7": "int64", "i7": "float", "cD": "object"}
    

    timeStamp = meta["modified"]
    Datenstand = dt.datetime.fromtimestamp(timeStamp / 1000)
    Datenstand = Datenstand.replace(hour=0, minute=0, second=0, microsecond=0)

    base_path = os.path.dirname(os.path.abspath(__file__))
        
    BL = ut.read_file(meta["BL_url"])
    LK = ut.read_file(meta["LK_url"])
    
    # for smaler files rename fields
    # i = Id(Landkreis or Bundesland)
    # t = Name of Id(Landkreis or Bundesland)
    # m = Meldedatum
    # c = cases
    # d = deaths
    # r = recovered
    # c7 = cases7d (cases7days)
    # i7 = incidence7d (incidence7days)

    LK.rename(columns={"IdLandkreis": "i", "Landkreis": "t", "Meldedatum": "m", "cases": "c", "deaths": "d", "recovered": "r", "cases7d": "c7", "incidence7d": "i7"}, inplace=True)
    BL.rename(columns={"IdBundesland": "i", "Bundesland": "t", "Meldedatum": "m","cases": "c", "deaths": "d", "recovered": "r", "cases7d": "c7", "incidence7d": "i7"}, inplace=True)
    
    # split LK
    LKcases = LK.copy()
    LKcases.drop(["t", "d", "r", "c7", "i7"], inplace=True, axis=1)
    LKcases["c"] = LKcases["c"].astype("int64")
    LKdeaths = LK.copy()
    LKdeaths.drop(["t", "c", "r", "c7", "i7"], inplace=True, axis=1)
    LKdeaths["d"] = LKdeaths["d"].astype("int64")
    LKrecovered = LK.copy()
    LKrecovered.drop(["t", "c", "d", "c7", "i7"], inplace=True, axis=1)
    LKrecovered["r"] = LKrecovered["r"].astype("int64")
    LKincidence = LK.copy()
    LKincidence.drop(["t", "c", "d", "r"], inplace=True, axis=1)
    LKincidence["c7"] = LKincidence["c7"].astype("int64")
    # free memory
    LK = pd.DataFrame()
    del LK
    gc.collect()

    # split BL
    BLcases = BL.copy()
    BLcases.drop(["t", "d", "r", "c7", "i7"], inplace=True, axis=1)
    BLcases["c"] = BLcases["c"].astype("int64")
    BLdeaths = BL.copy()
    BLdeaths.drop(["t", "c", "r", "c7", "i7"], inplace=True, axis=1)
    BLdeaths["d"] = BLdeaths["d"].astype("int64")
    BLrecovered = BL.copy()
    BLrecovered.drop(["t", "c", "d", "c7", "i7"], inplace=True, axis=1)
    BLrecovered["r"] = BLrecovered["r"].astype("int64")
    BLincidence = BL.copy()
    BLincidence.drop(["t", "c", "d", "r"], inplace=True, axis=1)
    BLincidence["c7"] = BLincidence["c7"].astype("int64")
    # free memory
    BL = pd.DataFrame()
    del BL
    gc.collect()

    # store all files not compressed! will be done later
    historyPath = os.path.normpath(os.path.join(base_path, "..", "dataStore", "history"))
    
    LKJsonFile = "districts.json"
    BLJsonFile = "states.json"
    
    CasesPath = os.path.join(historyPath, "cases")
    DeathsPath = os.path.join(historyPath, "deaths")
    RecoveredPath = os.path.join(historyPath, "recovered")
    IncidencePath = os.path.join(historyPath, "incidence")
    
    LKcasesJsonFull = os.path.join(CasesPath, LKJsonFile)
    LKdeathsJsonFull = os.path.join(DeathsPath, LKJsonFile)
    LKrecoveredJsonFull = os.path.join(RecoveredPath, LKJsonFile)
    LKincidenceJsonFull = os.path.join(IncidencePath, LKJsonFile)
    
    BLcasesJsonFull = os.path.join(CasesPath, BLJsonFile)
    BLdeathsJsonFull = os.path.join(DeathsPath, BLJsonFile)
    BLrecoveredJsonFull = os.path.join(RecoveredPath, BLJsonFile)
    BLincidenceJsonFull = os.path.join(IncidencePath, BLJsonFile)

    # read oldLK(cases, deaths, recovered, incidence) if old file exist
    # write new data 
    if os.path.exists(LKcasesJsonFull):
        oldLKcases = ut.read_json(fn=LKJsonFile, path=CasesPath, dtype=HC_dtp)
        oldLKcases["c"] = oldLKcases["c"].astype("int64")
    ut.write_json(df=LKcases, pt=CasesPath, fn=LKJsonFile)
    
    if os.path.exists(LKdeathsJsonFull):
        oldLKdeaths = ut.read_json(fn=LKJsonFile, path=DeathsPath, dtype=HD_dtp)
        oldLKdeaths["d"] = oldLKdeaths["d"].astype("int64")
    ut.write_json(df=LKdeaths, pt=DeathsPath, fn=LKJsonFile)
        
    if os.path.exists(LKrecoveredJsonFull):
        oldLKrecovered = ut.read_json(fn=LKJsonFile, path=RecoveredPath, dtype=HR_dtp)
        oldLKrecovered["r"] = oldLKrecovered["r"].astype("int64")
    ut.write_json(df=LKrecovered, pt=RecoveredPath, fn=LKJsonFile)
        
    if os.path.exists(LKincidenceJsonFull):
        oldLKincidence = ut.read_json(fn=LKJsonFile, path=IncidencePath, dtype=HI_dtp)
        oldLKincidence["c7"] = oldLKincidence["c7"].astype("int64")
    ut.write_json(df=LKincidence, pt=IncidencePath, fn=LKJsonFile)
        
    # read oldBL(cases, deaths, recovered, incidence) if old file exist
    # write new data
    if os.path.exists(BLcasesJsonFull):
        oldBLcases = ut.read_json(fn=BLJsonFile, path=CasesPath, dtype=HC_dtp)
        oldBLcases["c"] = oldBLcases["c"].astype("int64")
    ut.write_json(df=BLcases, pt=CasesPath, fn=BLJsonFile)
       
    if os.path.exists(BLdeathsJsonFull):
        oldBLdeaths = ut.read_json(fn=BLJsonFile, path=DeathsPath, dtype=HD_dtp)
        oldBLdeaths["d"] = oldBLdeaths["d"].astype("int64")
    ut.write_json(df=BLdeaths, pt=DeathsPath, fn=BLJsonFile)
       
    if os.path.exists(BLrecoveredJsonFull):
        oldBLrecovered = ut.read_json(fn=BLJsonFile, path=RecoveredPath, dtype=HR_dtp)
        oldBLrecovered["r"] = oldBLrecovered["r"].astype("int64")
    ut.write_json(df=BLrecovered, pt=RecoveredPath, fn=BLJsonFile)
    
    if os.path.exists(BLincidenceJsonFull):
        oldBLincidence = ut.read_json(fn=BLJsonFile, path=IncidencePath, dtype=HI_dtp)
        oldBLincidence["c7"] = oldBLincidence["c7"].astype("int64")
    ut.write_json(df=BLincidence, pt=IncidencePath, fn=BLJsonFile)

    # calculate diff data
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    print(aktuelleZeit, ": calculating history difference")
    changesPath = os.path.normpath(os.path.join(base_path, "..", "dataStore", "historychanges"))
    try:
        LKDiffCases = ut.get_different_rows(oldLKcases, LKcases)
        LKDiffCases.set_index(["i", "m"], inplace=True, drop=False)
        oldLKcases.set_index(["i","m"], inplace=True, drop=False)
        LKDiffCases["dc"] = LKDiffCases["c"] - oldLKcases["c"]
        LKDiffCases["dc"] = LKDiffCases["dc"].fillna(LKDiffCases["c"])
    except:
        LKDiffCases = LKcases.copy()
        LKDiffCases["dc"] = LKDiffCases["c"]
    
    try:
        LKDiffDeaths = ut.get_different_rows(oldLKdeaths, LKdeaths)
    except:
        LKDiffDeaths = LKdeaths.copy()
    
    try:
        LKDiffRecovered = ut.get_different_rows(oldLKrecovered, LKrecovered)
    except:
        LKDiffRecovered = LKrecovered.copy()
    
    try:
        # dont compare float values
        oldLKincidence.drop("i7", inplace=True, axis=1)
        temp = LKincidence.copy()
        LKincidence.drop("i7", inplace=True, axis=1)
        LKDiffIncidence = ut.get_different_rows(oldLKincidence, LKincidence)
        LKDiffIncidence.set_index(["i","m"], inplace=True, drop=False)
        temp.set_index(["i","m"], inplace=True, drop=True)
        LKDiffIncidence["i7"] = temp["i7"]
        LKDiffIncidence.reset_index(inplace=True, drop=True)
    except:
        LKDiffIncidence = LKincidence.copy()

    try:
        BLDiffCases = ut.get_different_rows(oldBLcases, BLcases)
        BLDiffCases.set_index(["i", "m"], inplace=True, drop=False)
        oldBLcases.set_index(["i","m"], inplace=True, drop=False)
        BLDiffCases["dc"] = BLDiffCases["c"] - oldBLcases["c"]
        BLDiffCases["dc"] = BLDiffCases["dc"].fillna(BLDiffCases["c"])
    except:
        BLDiffCases = BLcases.copy()
        BLDiffCases["dc"] = BLDiffCases["c"]
    
    try:
        BLDiffDeaths = ut.get_different_rows(oldBLdeaths, BLdeaths)
    except:
        BLDiffDeaths = BLdeaths.copy()
    
    try:
        BLDiffRecovered = ut.get_different_rows(oldBLrecovered, BLrecovered)
    except:
        BLDiffRecovered = BLrecovered.copy()
    
    try:
        oldBLincidence.drop("i7", inplace=True, axis=1)
        temp = BLincidence.copy()
        BLincidence.drop("i7", inplace=True, axis=1)
        BLDiffIncidence = ut.get_different_rows(oldBLincidence, BLincidence)
        BLDiffIncidence.set_index(["i","m"], inplace=True, drop=False)
        temp.set_index(["i","m"], inplace=True, drop=True)
        BLDiffIncidence["i7"] = temp["i7"]
        BLDiffIncidence.reset_index(inplace=True, drop=True)
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
    
    LKDiffJsonFile = "districts_Diff.json"
    BLDiffJsonFile = "states_Diff.json"

    DiffCasesPath = os.path.join(changesPath, "cases")
    DiffDeathsPath = os.path.join(changesPath, "deaths")
    DiffRecoveredPath = os.path.join(changesPath, "recovered")
    DiffIncidencePath = os.path.join(changesPath, "incidence")
    
    LKDiffCasesJsonFull = os.path.join(DiffCasesPath, LKDiffJsonFile)
    LKDiffDeathsJsonFull = os.path.join(DiffDeathsPath, LKDiffJsonFile)
    LKDiffRecoveredJsonFull = os.path.join(DiffRecoveredPath, LKDiffJsonFile)
    LKDiffIncidenceJsonFull = os.path.join(DiffIncidencePath, LKDiffJsonFile)

    BLDiffCasesJsonFull = os.path.join(DiffCasesPath, BLDiffJsonFile)
    BLDiffDeathsJsonFull = os.path.join(DiffDeathsPath, BLDiffJsonFile)
    BLDiffRecoveredJsonFull = os.path.join(DiffRecoveredPath, BLDiffJsonFile)
    BLDiffIncidenceJsonFull = os.path.join(DiffIncidencePath, BLDiffJsonFile)
    
    if os.path.exists(LKDiffCasesJsonFull):
        LKoldDiffCases = ut.read_json(fn=LKDiffJsonFile, path=DiffCasesPath, dtype=HCC_dtp)
        LKoldDiffCases = LKoldDiffCases[LKoldDiffCases["cD"] != ChangeDateStr]
        LKDiffCases = pd.concat([LKoldDiffCases, LKDiffCases])
    LKDiffCases["dc"] = LKDiffCases["dc"].astype(int)
    LKDiffCases.sort_values(by=["i", "m", "cD"], inplace=True)
    LKDiffCases.reset_index(inplace=True, drop=True)
    ut.write_json(df=LKDiffCases, pt=DiffCasesPath, fn=LKDiffJsonFile)
        
    if os.path.exists(LKDiffDeathsJsonFull):
        LKoldDiffDeaths = ut.read_json(fn=LKDiffJsonFile, path=DiffDeathsPath, dtype= HCD_dtp)
        LKoldDiffDeaths = LKoldDiffDeaths[LKoldDiffDeaths["cD"] != ChangeDateStr]
        LKDiffDeaths = pd.concat([LKoldDiffDeaths, LKDiffDeaths])
    LKDiffDeaths.sort_values(by=["i", "m", "cD"], inplace=True)
    LKDiffDeaths.reset_index(inplace=True, drop=True)
    ut.write_json(df=LKDiffDeaths, pt=DiffDeathsPath, fn=LKDiffJsonFile)
        
    if os.path.exists(LKDiffRecoveredJsonFull):
        LKoldDiffRecovered = ut.read_json(fn=LKDiffJsonFile, path=DiffRecoveredPath, dtype= HCR_dtp)
        LKoldDiffRecovered = LKoldDiffRecovered[LKoldDiffRecovered["cD"] != ChangeDateStr]
        LKDiffRecovered = pd.concat([LKoldDiffRecovered, LKDiffRecovered])
    LKDiffRecovered.sort_values(by=["i", "m", "cD"], inplace=True)
    LKDiffRecovered.reset_index(inplace=True, drop=True)
    ut.write_json(df=LKDiffRecovered, pt=DiffRecoveredPath, fn=LKDiffJsonFile)
        
    if os.path.exists(LKDiffIncidenceJsonFull):
        LKoldDiffIncidence = ut.read_json(fn=LKDiffJsonFile, path=DiffIncidencePath, dtype= HCI_dtp)
        LKoldDiffIncidence = LKoldDiffIncidence[LKoldDiffIncidence["cD"] != ChangeDateStr]
        LKDiffIncidence = pd.concat([LKoldDiffIncidence, LKDiffIncidence])
    LKDiffIncidence.sort_values(by=["i", "m", "cD"], inplace=True)
    LKDiffIncidence.reset_index(inplace=True, drop=True)
    ut.write_json(df=LKDiffIncidence, pt=DiffIncidencePath, fn=LKDiffJsonFile)
    
    if os.path.exists(BLDiffCasesJsonFull):
        BLoldDiffCases = ut.read_json(fn=BLDiffJsonFile, path=DiffCasesPath, dtype=HCC_dtp)
        BLoldDiffCases = BLoldDiffCases[BLoldDiffCases["cD"] != ChangeDateStr]
        BLDiffCases = pd.concat([BLoldDiffCases, BLDiffCases])
    BLDiffCases["dc"] = BLDiffCases["dc"].astype(int)
    BLDiffCases.sort_values(by=["i", "m", "cD"], inplace=True)
    BLDiffCases.reset_index(inplace=True, drop=True)
    ut.write_json(df=BLDiffCases, pt=DiffCasesPath, fn=BLDiffJsonFile)
        
    if os.path.exists(BLDiffDeathsJsonFull):
        BLoldDiffDeaths = ut.read_json(fn=BLDiffJsonFile, path=DiffDeathsPath, dtype= HCD_dtp)
        BLoldDiffDeaths = BLoldDiffDeaths[BLoldDiffDeaths["cD"] != ChangeDateStr]
        BLDiffDeaths = pd.concat([BLoldDiffDeaths, BLDiffDeaths])
    BLDiffDeaths.sort_values(by=["i", "m", "cD"], inplace=True)
    BLDiffDeaths.reset_index(inplace=True, drop=True)
    ut.write_json(df=BLDiffDeaths, pt=DiffDeathsPath, fn=BLDiffJsonFile)
        
    if os.path.exists(BLDiffRecoveredJsonFull):
        BLoldDiffRecovered = ut.read_json(fn=BLDiffJsonFile, path=DiffRecoveredPath, dtype= HCR_dtp)
        BLoldDiffRecovered = BLoldDiffRecovered[BLoldDiffRecovered["cD"] != ChangeDateStr]
        BLDiffRecovered = pd.concat([BLoldDiffRecovered, BLDiffRecovered])
    BLDiffRecovered.sort_values(by=["i", "m", "cD"], inplace=True)
    BLDiffRecovered.reset_index(inplace=True, drop=True)
    ut.write_json(df=BLDiffRecovered, pt=DiffRecoveredPath, fn=BLDiffJsonFile)
        
    if os.path.exists(BLDiffIncidenceJsonFull):
        BLoldDiffIncidence = ut.read_json(fn=BLDiffJsonFile, path=DiffIncidencePath, dtype= HCI_dtp)
        BLoldDiffIncidence = BLoldDiffIncidence[BLoldDiffIncidence["cD"] != ChangeDateStr]
        BLDiffIncidence = pd.concat([BLoldDiffIncidence, BLDiffIncidence])
    BLDiffIncidence.sort_values(by=["i", "m", "cD"], inplace=True)
    BLDiffIncidence.reset_index(inplace=True, drop=True)
    ut.write_json(df=BLDiffIncidence, pt=DiffIncidencePath, fn=BLDiffJsonFile)
    return