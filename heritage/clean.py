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
    claim.PayDelay = claim.PayDelay.str.replace('\+', '').fillna('0').astype(np.int)
    claim.CharlsonIndex  = claim.CharlsonIndex.fillna(-1).str.replace("[-|+].*", "").astype(np.int32)
    # claim.Specialty = claim.Specialty.fillna('0')
    # claim.PlaceSvc = claim.PlaceSvc.fillna('0')
    # claim.PrimaryConditionGroup = claim.PrimaryConditionGroup.fillna('0')
    # claim.Year = claim.Year.str.replace('Y', '').fillna('0').astype(np.int)
    # claim.LengthOfStay = claim.LengthOfStay.str.replace(' day[s]*', '').fillna('0')
    # claim.LengthOfStay = claim.LengthOfStay.str.replace('1- 2 weeks', '10.5')
    # claim.LengthOfStay = claim.LengthOfStay.str.replace('2- 4 weeks', '21')
    # claim.LengthOfStay = claim.LengthOfStay.str.replace('4- 8 weeks', '42')
    # claim.LengthOfStay = claim.LengthOfStay.str.replace('26\+ weeks', '182').astype(np.float)
    # claim.DSFS = normalizeMonths(claim.DSFS)
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

