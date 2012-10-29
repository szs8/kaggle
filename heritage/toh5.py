#!/usr/bin/env python

"""
Convert the claims data to .h5 pandas HDF store.

python toh5.py [--safe]

Use --safe flag if you are running out of memory
"""

import clean
import util
import sys

def safe_read(file_name):
    split_frame = pd.read_csv(file_name,iterator = True,chunksize = 1000)
    return pd.concat(split_frame,ignore_index = True)

def create_h5_file(datadir,filepath):
    files = {"claim": "Claims.csv",
            "drug":"DrugCount.csv",
            "lab":"LabCount.csv",
            "member": "Members.csv",
            "dih"  : "DaysInHospital_Y2.csv",
            "dih_y3" :"DaysInHospital_Y3.csv"
            }
    store = pd.HDFStore(filepath, 'w')
    for category in files:
        store[category] = safe_read(datadir + "/" + files[category])
    store.close()

if sys.argv[-1] == '--safe':
    create_h5_file("./data","HHP_release3.h5")
else:
    data = clean.loadRawData('./data')
    data['claim'] = clean.cleanClaim(data['claim'])
    util.storeAsH5('./data/HHP_release3.h5', data)
