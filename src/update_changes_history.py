import os
import datetime as dt
import pandas as pd
import utils as ut
import gc

def update(Datenstand):
    base_path = os.path.dirname(os.path.abspath(__file__))
    base_data_url = "https://raw.githubusercontent.com/Rubber1Duck/RD_RKI_COVID19_DATA/master/dataStore/historychanges/"
    BL_base_url = base_data_url + "BL_BaseData.feather"
    LK_base_url = base_data_url + "LK_BaseData.feather"
    
    BL = ut.read_file(BL_base_url)
    LK = ut.read_file(LK_base_url)
    
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
        LKDiffIncidence = ut.get_different_rows(oldLKincidence, LKincidence)
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
    LKDiffCases["dc"] = LKDiffCases["dc"].astype(int)
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
    BLDiffCases["dc"] = BLDiffCases["dc"].astype(int)
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