#!/usr/bin/env python
import pandas as pd
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
create_h5_file("./data","HHP_release3.h5")
