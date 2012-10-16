#!/usr/bin/env python
import numpy as np
import pandas as pd
import clean
import datetime
import logging
logging.basicConfig(level=logging.DEBUG)
np.random.seed(37)
def count_drug_lab(data):
    """
    This function takes in a daterame and appends count of drug and lab per DSFS
    DataFrame returned is MultiIndex on MemberID and Year
    """
    fields       = data["DSFS"].unique()
    new_frame   = pd.DataFrame( [{"MemberID":x[0],"Year":x[1]} for x in sorted(data.set_index(["MemberID","Year"]).index.unique())])
    null_series = pd.Series(np.array(np.repeat(np.nan,len(new_frame.index)),dtype=np.object))
    null_frame = {}
    for field in fields:
        null_frame["Drug_"+field] = null_series
        null_frame["Lab_"+field] = null_series
    new_frame= new_frame.join(pd.DataFrame(null_frame))
    new_frame = new_frame.set_index(["MemberID","Year"])
    for row in data.iterrows():
        new_frame.ix[(row[1]["MemberID"],row[1]["Year"])]["Drug_"+row[1]["DSFS"]]  =  row[1]["DrugCount"]
        new_frame.ix[(row[1]["MemberID"],row[1]["Year"])]["Lab_"+row[1]["DSFS"]]  =  row[1]["LabCount"]
    return new_frame
def widen_on_fields(data,fields_counted):
    """
    This function takes in a dataFrame and list of fields.
    It iterates over fields and returns count each possible field occured per year
    DataFrame returned is MultiIndex on MemberID and Year
    """
    data_in    = data.groupby(["MemberID","Year"])
    counted_data = [] 
    for row,group in data_in:
        counted_by_index = pd.Series({"MemberID":row[0],"Year":row[1]})
        for field in fields_counted:
            counts = group.groupby(field).size().rename(lambda x: field+"_%s" %  str(x))
            counted_by_index = counted_by_index.append(counts)
        counted_data.append(counted_by_index)
    counted_dataFrame = pd.concat(counted_data,axis=1).transpose()
    new_frame = counted_dataFrame.set_index(["MemberID","Year"])
    return new_frame
def avg_and_max_fields(data,fields_concerned):
    """
    This function takes in a dataFrame and list of fields.
    It iterates over fields and returns field with max and avg for each item.
    DataFrame returned is MultiIndex on MemberID and Year
    """
    old_data = data
    new_frame   = pd.DataFrame( [{"MemberID":x[0],"Year":x[1]} for x in sorted(data.set_index(["MemberID","Year"]).index.unique())])
    null_frame =  {}
    for field in fields_concerned:
        old_data[field]   = old_data[field].str.replace("[-|+].*", "").astype(np.float64)
    new_frame = new_frame.join(pd.DataFrame(null_frame))
    new_frame = new_frame.set_index(["MemberID","Year"])
    #done this way instead of agg to to take advantage of cython optimization
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
def createFeatures():
    """
    This is just a sample script function showing the functionality of 
    widen_on_fields
    avg_and_max_fields
    """
    data   =  clean.readH5Store("HHP_release3.h5")
    claim  = data["claim"]
    drug   = data["drug"]
    lab    = data["lab"]
    member = data["member"]
    dih    = data["dih"]
    dih    = dih.join(pd.DataFrame({"Year":np.repeat("Y1",len(dih.index))}))
    dih_3  = data["dih_y3"]
    dih_3  = dih_3.join(pd.DataFrame({"Year":np.repeat("Y2",len(dih_3.index))}))
    days_in_hospital   = dih.append(dih_3)
    days_in_hospital = days_in_hospital.set_index(["MemberID","Year"])
    days_in_hospital.columns =  ["NextYearTruncated","Target"]
    drug   = member.merge(drug,on="MemberID")
    drug_lab = drug.merge(lab,how="outer",on=["MemberID","Year","DSFS"])
    drug_start = datetime.datetime.now()
    drug_lab_count = count_drug_lab(drug_lab)# now indexed by MemberID and Year
    drug_end   = datetime.datetime.now()
    logging.debug("drug_done.  IT took %d seconds" % (drug_end - drug_start).seconds)
    claim_counting_fields = ["Specialty", "PlaceSvc","LengthOfStay", "PrimaryConditionGroup", "CharlsonIndex","ProcedureGroup"]
    widen_start = datetime.datetime.now()
    claims_counted = widen_on_fields(claim,claim_counting_fields)
    widen_end = datetime.datetime.now()
    logging.debug("widen_done.  it took %d seconds"% (widen_end - widen_start).seconds)
    avg_fields = ["PayDelay"]
    avg_start = datetime.datetime.now()
    avg_frame = avg_and_max_fields(claim,avg_fields)
    avg_end = datetime.datetime.now()
    logging.debug("avg_done.  it took %d seconds" % (avg_end - avg_start).seconds)
    features_frame = drug_lab_count.join(claims_counted,how="outer").join(avg_frame,how="outer").join(days_in_hospital,how="outer")
    data["features"] = features_frame
    clean.storeAsH5("HHP_release3.h5",data)

if __name__ == '__main__':
    createFeatures()
