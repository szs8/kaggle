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
        cols = data.ix[:, field]
        df = pd.crosstab(rows=rows, cols=cols)
        df.columns = [field + '_' + i for i in df.columns]
        res = df if res is None else res.join(df, how="outer")
    return res

data = clean.readH5Store("HHP_release3.h5")
claim  = data["claim"]
df = widen_on_fields(claim, ["Specialty", "PlaceSvc","LengthOfStay",
                             "PrimaryConditionGroup", "CharlsonIndex",
                             "ProcedureGroup"])
print df
print df[:5]
