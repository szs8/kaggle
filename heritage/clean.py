#!/usr/bin/env python

import numpy as np
import pandas as pd

import config

np.random.seed(37)

def readH5Store(filepath):
    store = pd.HDFStore(filepath, "r")
    r = dict((k, store[k]) for k in store.keys())
    store.close()
    return r

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

def prepareData():
    data = loadY1Data('r3y1.h5')
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

if __name__ == '__main__':
    prepareData()
