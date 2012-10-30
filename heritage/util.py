#!/usr/bin/env python

import numpy as np
import pandas as pd

import config

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
    mids = features.index.get_level_values("MemberID")
    trainID, valID, testID = partitionMembers(mids)
    features.reset_index(inplace=1)
    train = features[features.MemberID.isin(trainID)].set_index(["MemberID", "Year"])
    val = features[features.MemberID.isin(valID)].set_index(["MemberID", "Year"])
    test = features[features.MemberID.isin(testID)].set_index(["MemberID", "Year"])
    return train, val, test

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

def splitArray(arr, ratio=-.5, seed=None):
    """ Randomly split the given array into 2 pieces based
    on the given ratio. Note that this function first
    uniquifies the arr. """
    a = np.sort(np.unique(arr))
    n = len(a)
    if seed:  np.random.seed(seed)
    idx = np.sort(sample(n, int(np.floor(n*float(.5)))))
    return a[idx], a[np.setdiff1d(np.arange(n), idx)]

def partitionMembers(memberID):
    """ Partition MemberID into train, validation and test sets
    The current split is : 50% for trainin,  25% for validation, and
    25% for test  """
    train, rest = splitArray(memberID, 0.5, seed=47)
    val, test = splitArray(rest, 0.5, seed=52)
    return train, val, test

def randomDF():
    return pd.DataFrame({'a': np.arange(100), 'b': np.random.randn(100),
                         'c': np.random.randn(100)})
