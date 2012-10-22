#!/usr/bin/env python

import pandas as pd
import numpy as np
import clean

claim = clean.loadClaim()
claim = claim[claim["ProviderID"] > 0]
claim = claim[claim["Specialty"] != '0']
df = claim.groupby(["ProviderID", "Specialty"]).size()
df.to_csv("provider-speciality.csv", header=True)

# How many unique ProviderIDs does a member have?
print claim.groupby(["MemberID", "Year"])["ProviderID"].apply(lambda x : len(x.unique()))

# Is a member going to multiple providers having the same specialty?

# Can we predict if a member goes to the same ProviderID?

# What is the distribution of ProviderIDs?

# What is relationship between ProviderID and LengthOfStay?

# Does this relationship vary by year?

# Does a ProviderID have multiple Specialities?

# Is speciality related to ProviderID or to PCP?

# Is any particular combination of ProviderID and PCP predictive of LengthOfStay?

# If there are there any members who are older than say 75 and have claims in the earlier year but no claims in the next years?
# Perhaps they are dead?

