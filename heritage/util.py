#!/usr/bin/env python

import numpy as np
import pandas as pd

import config

np.random.seed(37)

def storeAsH5(filepath, data):
    """ Store a dict of DataFrames as a hdf datastore """
    store = pd.HDFStore(filepath, 'w')
    for k in data:
        store[k] = data[k]
    store.close()

def readH5Store(filepath):
    """ Read an entire hdf datastore and return a dict of DataFrames """
    store = pd.HDFStore(filepath, "r")
    r = dict((k, store[k]) for k in store.keys())
    store.close()
    return r

def loadTable(filepath, table):
    """ Load a particular DataFrame from a HDF datastore """
    store = pd.HDFStore(filepath, "r")
    r = store[table]
    store.close()
    return r

def loadFeatures(filepath='HHP_release3.h5'):
    return loadTable(filepath, 'features')

def splitFeatures(features):
    """ Split features into train, validation, and test pieces """
    mids = pd.Series(features.index.get_level_values("MemberID"))
    trainID, valID, testID = partitionMembers(mids)
    tidx = np.searchsorted(features.index.get_level_values("MemberID"), trainID)
    vidx = np.searchsorted(features.index.get_level_values("MemberID"), valID)
    testidx = np.searchsorted(features.index.get_level_values("MemberID"), testID)
    return features.ix[tidx], features.ix[vidx], features.ix[testidx]

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
