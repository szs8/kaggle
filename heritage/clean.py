#!/usr/bin/env python

import numpy as np
import pandas as pd

import config

np.random.seed(37)

def loadRawData(datadir):
    """ Read HHP_release3.zip provided by kaggle """
    r = {}
    r['claim'] = pd.read_csv(datadir + "/Claims.csv")
    r['drug'] = pd.read_csv(datadir + "/DrugCount.csv")
    r['lab'] = pd.read_csv(datadir + "/LabCount.csv")
    r['member'] = pd.read_csv(datadir + "/Members.csv")
    r['dih'] = pd.read_csv(datadir + "/DaysInHospital_Y2.csv")
    r['dih_y3'] = pd.read_csv(datadir + "/DaysInHospital_Y3.csv")
    return r

def storeAsH5(filepath, data):
    store = pd.HDFStore(filepath, 'w')
    for k in data:
        store[k] = data[k]
    store.close()

def readH5Store(filepath):
    store = pd.HDFStore(filepath, "r")
    r = dict((k, store[k]) for k in store.keys())
    store.close()
    return r

def loadTable(filepath, table):
    store = pd.HDFStore(filepath, "r")
    r = store[table]
    store.close()
    return r

def loadFeatures(filepath='HHP_release3.h5'):
    return loadTable(filepath, 'features')

def loadY1Data(filepath='r3y1.h5'):
    """ Raw data provided by kaggle """
    return readH5Store(filepath=filepath)

def loadData(dataset='TRAIN'):
    if dataset not in ('TRAIN', 'VALIDATION', 'TEST'):
        raise ValueError("Unknown dataset %s" % (dataset, ))
    return readH5Store(getattr(config, dataset))

def loadClaim(dataset='TRAIN'):
    data = loadData(dataset)
    return data['claim']

def loadDrug(dataset='TRAIN'):
    data = loadData(dataset)
    return data['drug']

def loadLab(dataset='TRAIN'):
    data = readH5Store(getattr(config, dataset))
    return data['lab']

def sample(n, k):
    """ Random sampling without replacement
    Choose k indices from arange(k) """
    return np.random.permutation(n)[:k]

def splitSeries(series, ratio=.5):
    """ Randomly split a pandas series into 2 series based on the given ratio """
    s = pd.Series(series.unique())
    n = len(s)
    idx = sample(n, int(np.floor(n*float(ratio))))
    a = s[idx]
    b = s.drop(a.index)
    return a.reset_index(drop=1), b.reset_index(drop=1)

def partitionMembers(memberID):
    """ Partition MemberID into train, validation and test sets
    The current split is :
    50% for training
    25% for validation
    25% for test  """
    train, remaining = splitSeries(memberID)
    validation, test = splitSeries(remaining)
    return train, validation, test

def normalizeMonths(series):
    idx = np.nonzero(series.isnull())
    r  = series.str.replace(" month[s]*", "").str.split("-")
    r = 30 * (r.fillna('0').str.get(0).astype(np.int) + 0.5)
    r.ix[idx] = 0
    return r.astype(np.int)

def normalizeAge(series):
    idx = np.nonzero(series.isnull())
    r  = series.fillna('0').str.replace('\+', '').str.split("-")
    r = r.str.get(0).astype(np.int) + 4.5
    r.ix[idx] = 0.
    return r

def cleanClaim(claim):
    claim.ProviderID = claim.ProviderID.fillna(0).astype(np.int)
    claim.Vendor = claim.Vendor.fillna(0).astype(np.int)
    claim.PCP = claim.PCP.fillna(0).astype(np.int)
    claim.Specialty = claim.Specialty.fillna('0')
    claim.PlaceSvc = claim.PlaceSvc.fillna('0')
    claim.PrimaryConditionGroup = claim.PrimaryConditionGroup.fillna('0')
    claim.Year = claim.Year.str.replace('Y', '').fillna('0').astype(np.int)
    claim.PayDelay = claim.PayDelay.str.replace('\+', '').fillna('0').astype(np.int)
    claim.LengthOfStay = claim.LengthOfStay.str.replace(' day[s]*', '').fillna('0')
    claim.LengthOfStay = claim.LengthOfStay.str.replace('1- 2 weeks', '10.5')
    claim.LengthOfStay = claim.LengthOfStay.str.replace('2- 4 weeks', '21')
    claim.LengthOfStay = claim.LengthOfStay.str.replace('4- 8 weeks', '42')
    claim.LengthOfStay = claim.LengthOfStay.str.replace('26\+ weeks', '182').astype(np.float)
    claim.DSFS = normalizeMonths(claim.DSFS)
    claim.CharlsonIndex  = claim.CharlsonIndex.str.replace("[-|+].*", "").astype(np.int32)
    return claim

def cleanMember(member):
    member.AgeAtFirstClaim = normalizeAge(member.AgeAtFirstClaim)
    member.Sex[member.Sex == 'F'] = '0'
    member.Sex[member.Sex == 'M'] = '1'
    member.Sex = member.Sex.fillna('0.5').astype(np.float)
    return member

def cleanDrug(drug):
    drug.Year = drug.Year.str.replace('Y', '').fillna('0').astype(np.int)
    drug.DSFS = normalizeMonths(drug.DSFS)
    drug.DrugCount = drug.DrugCount.str.replace('\+', '').astype(np.int)
    return drug

def cleanLab(lab):
    lab.Year = lab.Year.str.replace('Y', '').fillna('0').astype(np.int)
    lab.DSFS = normalizeMonths(lab.DSFS)
    lab.LabCount = lab.LabCount.str.replace('\+', '').astype(np.int)
    return lab

def cleanData(data):
    data['claim'] = cleanClaim(data['claim'])
    data['member'] = cleanMember(data['member'])
    data['drug'] = cleanDrug(data['drug'])
    data['lab'] = cleanLab(data['lab'])

def partitionData(data):
    """ claim, drug, lab are the datasets with features
    member has some meta information on members like age, sex etc.
    dih has the target (DaysInHospital)
    """
    trainID, validationID, testID = partitionMembers(data['claim'].MemberID)
    claim, drug, lab = data['claim'], data['drug'], data['lab']

    train, validation, test = {}, {}, {}
    train['claim'] = claim[claim.MemberID.isin(trainID)]
    train['drug'] = drug[drug.MemberID.isin(trainID)]
    train['lab'] = lab[lab.MemberID.isin(trainID)]

    validation['claim'] = claim[claim.MemberID.isin(validationID)]
    validation['drug'] = drug[drug.MemberID.isin(validationID)]
    validation['lab'] = lab[lab.MemberID.isin(validationID)]

    test['claim'] = claim[claim.MemberID.isin(testID)]
    test['drug'] = drug[drug.MemberID.isin(testID)]
    test['lab'] = lab[lab.MemberID.isin(testID)]

    return train, validation, test

def align(frame, member, dih):
    """ frame is any dataframe with MemberID that has to be aligned
    with member and dih """
    data = pd.merge(frame, member, on=['MemberID'], how="left", sort=False)
    return pd.merge(data, dih, on=['MemberID'], how="left", sort=False)

def prepareData1():
    data = loadY1Data('r3y1.h5')
    cleanData(data)
    train, validation, test = partitionData(data)
    store = pd.HDFStore("r3y1_train.h5", 'w')
    for k, v in train.iteritems():
        store[k] = align(v, data['member'], data['dih'])
    store.close()

    store = pd.HDFStore("r3y1_validation.h5", 'w')
    for k, v in validation.iteritems():
        store[k] = align(v, data['member'], data['dih'])
    store.close()

    store = pd.HDFStore("r3y1_test.h5", 'w')
    for k, v in test.iteritems():
        store[k] = align(v, data['member'], data['dih'])
    store.close()

def prepareData():
    data = readH5Store('HHP_release3.h5')
    cleanData(data)
    train, validation, test = partitionData(data)
    store = pd.HDFStore("HHP3_train.h5", 'w')
    for k, v in train.iteritems():
        store[k] = align(v, data['member'], data['dih'])
    store.close()

    store = pd.HDFStore("HHP3_validation.h5", 'w')
    for k, v in validation.iteritems():
        store[k] = align(v, data['member'], data['dih'])
    store.close()

    store = pd.HDFStore("HHP3_test.h5", 'w')
    for k, v in test.iteritems():
        store[k] = align(v, data['member'], data['dih'])
    store.close()

if __name__ == '__main__':
    prepareData()
