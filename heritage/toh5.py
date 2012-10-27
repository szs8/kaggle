#!/usr/bin/env python

import clean

data = clean.loadRawData('./data')
data['claim'] = clean.cleanClaim(data['claim'])
clean.storeAsH5('HHP_release3.h5', data)
