#!/usr/bin/env python

import pandas as pd
import numpy as np
import clean

ABBREVIATIONS = {"Specialty" : "S",  "PrimaryConditionGroup" : "PCG",
                 "ProcedureGroup" : "PG", "PlaceSvc" : "PS",
                 "LengthOfStay" : 'LOS', "CharlsonIndex" : "CI"}

def normalize_column_name(field, label):
    """
    field : The actual field name or list of field names.
            eg "Specialty" or ["Specialty", "PlaceSvc"]
    label : The label that pandas has automatically generated """
    flat_fields = np.ravel(np.array([field]))
    prefix = "_".join(ABBREVIATIONS.get(i, i) for i in flat_fields)
    suffix = "_".join(eval(label)) if len(flat_fields) > 1 else label
    suffix = suffix.replace(" and ", "And").replace(" ", "")
    return prefix + "_" + suffix

def widen_on_fields(data, fields_counted):
    """
    data :            pandas dataframe
    fields_counted :  List of fields to widen

    This function takes in a dataFrame and list of fields.
    It iterates over fields and returns a count for each possible field
    occured per year. DataFrame returned is MultiIndex on MemberID and Year """
    rows = [data.MemberID, data.Year]
    res = None
    for field in fields_counted:
        isList = 1 if isinstance(field, (list, tuple, np.ndarray)) else 0
        cols = [data.ix[:, i] for i in field] if isList else data.ix[:, field]
        df = pd.crosstab(rows=rows, cols=cols)
        df.columns = [normalize_column_name(field, str(i)) for i in df.columns]
        res = df if res is None else res.join(df, how="outer")
    return res.reset_index().set_index(["MemberID","Year"])

def avg_and_max_fields(data,fields_concerned):
    """
    This function takes in a dataFrame and list of fields.
    It iterates over fields and returns field with max and avg for each item.
    DataFrame returned is MultiIndex on MemberID and Year
    """
    old_data = data
    new_frame   = pd.DataFrame( [{"MemberID":x[0],"Year":x[1]} for x in sorted(data.set_index(["MemberID","Year"]).index.unique())])
    null_frame =  {}
    new_frame = new_frame.join(pd.DataFrame(null_frame))
    new_frame = new_frame.set_index(["MemberID","Year"])
    means = old_data.groupby(["MemberID","Year"]).mean()
    cols = []
    for column in means.columns:
        cols.append("Mean_"+column)
    means.columns = cols
    cols = []
    maxs  = old_data.groupby(["MemberID","Year"]).max()
    for column in maxs:
        cols.append("Max_"+column)
    maxs.columns = cols
    for field in fields_concerned:
        new_frame = new_frame.join(means["Mean_"+field])
        new_frame = new_frame.join(maxs["Max_"+field])
    return new_frame

if __name__ == '__main__':
    data = clean.readH5Store("HHP_release3.h5")
    claim  = data["claim"]
    df = widen_on_fields(claim, ["Specialty", "PlaceSvc", "LengthOfStay",
                                 "PrimaryConditionGroup", "ProcedureGroup"])
                                 # ["Specialty", "PlaceSvc"],
                                 # ["ProcedureGroup", "PrimaryConditionGroup", "Specialty"]])
    store = pd.HDFStore("feat.h5", 'w')
    store['feat'] = df
    store.close()

    # data["features"] = df
    # clean.storeAsH5("HHP_release3.h5", data)
