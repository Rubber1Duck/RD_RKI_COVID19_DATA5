import os
import datetime as dt
import pandas as pd
import utils as ut
import gc

def update(meta):
    HC_dtp = {"i": "str", "m": "object", "c": "int64"}
    HD_dtp = {"i": "str", "m": "object", "d": "int64"}
    HR_dtp = {"i": "str", "m": "object", "r": "int64"}
    HI_dtp = {"i": "str", "m": "object", "c7": "int64"}

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
    LKcases = LK[["i", "m", "c"]].copy()
    LKdeaths = LK[["i", "m", "d"]].copy()
    LKrecovered = LK[["i", "m", "r"]].copy()
    LKc7 = LK[["i", "m", "c7"]].copy()
    LKi7 = LK[["i", "m", "i7"]].copy()

    # free memory
    LK = pd.DataFrame()
    del LK
    gc.collect()

    # split BL
    BLcases = BL[["i", "m", "c"]].copy()
    BLdeaths = BL[["i", "m", "d"]].copy()
    BLrecovered = BL[["i", "m", "r"]].copy()
    BLc7 = BL[["i", "m", "c7"]].copy()
    BLi7 = BL[["i", "m", "i7"]].copy()
    # free memory
    BL = pd.DataFrame()
    del BL
    gc.collect()

    # store all files not compressed! will be done later
    LKcasesJsonFull = os.path.join(base_path, "..", "dataStore", "history", "cases", "districts.json.xz")
    LKdeathsJsonFull = os.path.join(base_path, "..", "dataStore", "history", "deaths", "districts.json.xz")
    LKrecoveredJsonFull = os.path.join(base_path, "..", "dataStore", "history", "recovered", "districts.json.xz")
    LKincidenceJsonFull = os.path.join(base_path, "..", "dataStore", "history", "incidence", "districts.json.xz")

    BLcasesJsonFull = os.path.join(base_path, "..", "dataStore", "history", "cases", "states.json.xz")
    BLdeathsJsonFull = os.path.join(base_path, "..", "dataStore", "history", "deaths", "states.json.xz")
    BLrecoveredJsonFull = os.path.join(base_path, "..", "dataStore", "history", "recovered", "states.json.xz")
    BLincidenceJsonFull = os.path.join(base_path, "..", "dataStore", "history", "incidence", "states.json.xz")

    # calculate diff data
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    print(aktuelleZeit, ": calculating history difference")

    # LK data
    oldLKcases = ut.read_json(full_fn=LKcasesJsonFull, dtype=HC_dtp)
    ut.write_json(df=LKcases, full_fn=LKcasesJsonFull[:-3])
    LKDiffCases = ut.get_different_rows(oldLKcases, LKcases)
    LKDiffCases.set_index(["i", "m"], inplace=True, drop=False)
    oldLKcases.set_index(["i","m"], inplace=True, drop=False)
    LKDiffCases["dc"] = LKDiffCases["c"] - oldLKcases["c"]
    LKDiffCases["dc"] = LKDiffCases["dc"].fillna(LKDiffCases["c"]).astype(int)

    oldLKdeaths = ut.read_json(full_fn=LKdeathsJsonFull, dtype=HD_dtp)
    ut.write_json(df=LKdeaths, full_fn=LKdeathsJsonFull[:-3])
    LKDiffDeaths = ut.get_different_rows(oldLKdeaths, LKdeaths)

    oldLKrecovered = ut.read_json(full_fn=LKrecoveredJsonFull, dtype=HR_dtp)
    ut.write_json(df=LKrecovered, full_fn=LKrecoveredJsonFull[:-3])
    LKDiffRecovered = ut.get_different_rows(oldLKrecovered, LKrecovered)

    oldLKincidence = ut.read_json(full_fn=LKincidenceJsonFull, dtype=HI_dtp)
    ut.write_json(df=LKc7, full_fn=LKincidenceJsonFull[:-3])
    # dont compare float values
    if 'i7' in oldLKincidence.columns:
        oldLKincidence.drop(columns=["i7"], inplace=True)
    LKDiffIncidence = ut.get_different_rows(oldLKincidence, LKc7)
    LKDiffIncidence.set_index(["i","m"], inplace=True, drop=False)
    LKi7.set_index(["i","m"], inplace=True, drop=True)
    LKDiffIncidence["i7"] = LKi7["i7"]
    LKDiffIncidence.reset_index(inplace=True, drop=True)

    # BL data
    oldBLcases = ut.read_json(full_fn=BLcasesJsonFull, dtype=HC_dtp)
    ut.write_json(df=BLcases, full_fn=BLcasesJsonFull[:-3])
    BLDiffCases = ut.get_different_rows(oldBLcases, BLcases)
    BLDiffCases.set_index(["i", "m"], inplace=True, drop=False)
    oldBLcases.set_index(["i","m"], inplace=True, drop=False)
    BLDiffCases["dc"] = BLDiffCases["c"] - oldBLcases["c"]
    BLDiffCases["dc"] = BLDiffCases["dc"].fillna(BLDiffCases["c"]).astype(int)

    oldBLdeaths = ut.read_json(full_fn=BLdeathsJsonFull, dtype=HD_dtp)
    ut.write_json(df=BLdeaths, full_fn=BLdeathsJsonFull[:-3])
    BLDiffDeaths = ut.get_different_rows(oldBLdeaths, BLdeaths)

    oldBLrecovered = ut.read_json(full_fn=BLrecoveredJsonFull, dtype=HR_dtp)
    ut.write_json(df=BLrecovered, full_fn=BLrecoveredJsonFull[:-3])
    BLDiffRecovered = ut.get_different_rows(oldBLrecovered, BLrecovered)

    oldBLincidence = ut.read_json(full_fn=BLincidenceJsonFull, dtype=HI_dtp)
    ut.write_json(df=BLc7, full_fn=BLincidenceJsonFull[:-3])
    if 'i7' in oldBLincidence.columns:
        oldBLincidence.drop(columns=["i7"], inplace=True)
    BLDiffIncidence = ut.get_different_rows(oldBLincidence, BLc7)
    BLDiffIncidence.set_index(["i","m"], inplace=True, drop=False)
    BLi7.set_index(["i","m"], inplace=True, drop=True)
    BLDiffIncidence["i7"] = BLi7["i7"]
    BLDiffIncidence.reset_index(inplace=True, drop=True)

    ChangeDate = dt.datetime.strftime(Datenstand, "%Y-%m-%d")
    LKDiffCases["cD"] = ChangeDate
    LKDiffDeaths["cD"] = ChangeDate
    LKDiffRecovered["cD"] = ChangeDate
    LKDiffIncidence["cD"] = ChangeDate

    BLDiffCases["cD"] = ChangeDate
    BLDiffDeaths["cD"] = ChangeDate
    BLDiffRecovered["cD"] = ChangeDate
    BLDiffIncidence["cD"] = ChangeDate

    LKDiffCasesJsonFull = os.path.join(base_path, "..", "dataStore", "historychanges", "cases", "districts_Diff.json.xz")
    LKDiffDeathsJsonFull = os.path.join(base_path, "..", "dataStore", "historychanges", "deaths", "districts_Diff.json.xz")
    LKDiffRecoveredJsonFull = os.path.join(base_path, "..", "dataStore", "historychanges", "recovered", "districts_Diff.json.xz")
    LKDiffIncidenceJsonFull = os.path.join(base_path, "..", "dataStore", "historychanges", "incidence", "districts_Diff.json.xz")

    BLDiffCasesJsonFull = os.path.join(base_path, "..", "dataStore", "historychanges", "cases", "states_Diff.json.xz")
    BLDiffDeathsJsonFull = os.path.join(base_path, "..", "dataStore", "historychanges", "deaths", "states_Diff.json.xz")
    BLDiffRecoveredJsonFull = os.path.join(base_path, "..", "dataStore", "historychanges", "recovered", "states_Diff.json.xz")
    BLDiffIncidenceJsonFull = os.path.join(base_path, "..", "dataStore", "historychanges", "incidence", "states_Diff.json.xz")

    LKoldDiffCases = ut.read_json(full_fn=LKDiffCasesJsonFull, dtype=HCC_dtp)
    LKDiffCases = pd.concat([LKoldDiffCases, LKDiffCases])
    LKDiffCases.sort_values(by=["i", "m", "cD"], inplace=True)
    ut.write_json(df=LKDiffCases, full_fn=LKDiffCasesJsonFull[:-3])

    LKoldDiffDeaths = ut.read_json(full_fn=LKDiffDeathsJsonFull, dtype= HCD_dtp)
    LKDiffDeaths = pd.concat([LKoldDiffDeaths, LKDiffDeaths])
    LKDiffDeaths.sort_values(by=["i", "m", "cD"], inplace=True)
    ut.write_json(df=LKDiffDeaths, full_fn=LKDiffDeathsJsonFull[:-3])

    LKoldDiffRecovered = ut.read_json(full_fn=LKDiffRecoveredJsonFull, dtype= HCR_dtp)
    LKDiffRecovered = pd.concat([LKoldDiffRecovered, LKDiffRecovered])
    LKDiffRecovered.sort_values(by=["i", "m", "cD"], inplace=True)
    ut.write_json(df=LKDiffRecovered, full_fn=LKDiffRecoveredJsonFull[:-3])

    LKoldDiffIncidence = ut.read_json(full_fn=LKDiffIncidenceJsonFull, dtype= HCI_dtp)
    LKDiffIncidence = pd.concat([LKoldDiffIncidence, LKDiffIncidence])
    LKDiffIncidence.sort_values(by=["i", "m", "cD"], inplace=True)
    ut.write_json(df=LKDiffIncidence, full_fn=LKDiffIncidenceJsonFull[:-3])

    BLoldDiffCases = ut.read_json(full_fn=BLDiffCasesJsonFull, dtype=HCC_dtp)
    BLDiffCases = pd.concat([BLoldDiffCases, BLDiffCases])
    BLDiffCases.sort_values(by=["i", "m", "cD"], inplace=True)
    ut.write_json(df=BLDiffCases, full_fn=BLDiffCasesJsonFull[:-3])

    BLoldDiffDeaths = ut.read_json(full_fn=BLDiffDeathsJsonFull, dtype= HCD_dtp)
    BLDiffDeaths = pd.concat([BLoldDiffDeaths, BLDiffDeaths])
    BLDiffDeaths.sort_values(by=["i", "m", "cD"], inplace=True)
    ut.write_json(df=BLDiffDeaths, full_fn=BLDiffDeathsJsonFull[:-3])

    BLoldDiffRecovered = ut.read_json(full_fn=BLDiffRecoveredJsonFull, dtype= HCR_dtp)
    BLDiffRecovered = pd.concat([BLoldDiffRecovered, BLDiffRecovered])
    BLDiffRecovered.sort_values(by=["i", "m", "cD"], inplace=True)
    ut.write_json(df=BLDiffRecovered, full_fn=BLDiffRecoveredJsonFull[:-3])

    BLoldDiffIncidence = ut.read_json(full_fn=BLDiffIncidenceJsonFull, dtype= HCI_dtp)
    BLDiffIncidence = pd.concat([BLoldDiffIncidence, BLDiffIncidence])
    BLDiffIncidence.sort_values(by=["i", "m", "cD"], inplace=True)
    ut.write_json(df=BLDiffIncidence, full_fn=BLDiffIncidenceJsonFull[:-3])
    return
