#!/usr/bin/env python

import pandas as pd

# Working with HHP_release3.zip
claim = pd.read_csv("data/Claims.csv")
drug = pd.read_csv("data/DrugCount.csv")
lab = pd.read_csv("data/LabCount.csv")
member = pd.read_csv("data/Members.csv")
dih = pd.read_csv("data/DaysInHospital_Y2.csv")

# extract data for Y1 and store as h5
store = pd.HDFStore("r3y1.h5", 'w')
store['claim'] = claim[claim.Year == "Y1"].reset_index(drop=1)
store['drug'] = drug[drug.Year == "Y1"].reset_index(drop=1)
store['lab'] = lab[lab.Year == "Y1"].reset_index(drop=1)
store['member'] = member
store['dih'] = dih
store.close()
