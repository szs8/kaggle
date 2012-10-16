#!/usr/bin/env python

import pandas as pd
import numpy as np
import clean

def widen_on_fields(data,fields_counted):
    """
    This function takes in a dataFrame and list of fields.
    It iterates over fields and returns count each possible field occured per year
    DataFrame returned is MultiIndex on MemberID and Year
    """
    rows = [data.MemberID, data.Year]
    res = None
    for field in fields_counted:
        isList = 1 if isinstance(field, (list, tuple, np.ndarray)) else 0
        cols = [data.ix[:, i] for i in field] if isList else data.ix[:, field]
        df = pd.crosstab(rows=rows, cols=cols)
        key = "_".join(field) if isList else field
        df.columns = [key + '_' + str(i) for i in df.columns]
        res = df if res is None else res.join(df, how="outer")
    return res

data = clean.readH5Store("HHP_release3.h5")
claim  = data["claim"]
df = widen_on_fields(claim, ["Specialty", "PlaceSvc","LengthOfStay",
                             "PrimaryConditionGroup", "CharlsonIndex",
                             "ProcedureGroup", ["Specialty", "PlaceSvc"]])

print len(df), "rows"
print df.columns

#print df[:5]
