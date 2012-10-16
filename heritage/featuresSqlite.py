'''
Construct features from sqlite3 tables
'''
import sqlite3
import pandas.io.sql as psql

def read_db(sql, con):
	return psql.frame_query(sql, con)

def constructFeatures():
	# PlaceSvcCount	
	with sqlite3.connect('data/sqldata') as conn:
		df_sqlite=read_db('select MemberID,Year,count(distinct(PlaceSvc)) as PlaceSvcCount FROM claims GROUP BY MemberID,Year;',con=conn)
	
	print('PlaceSvc done...')
	# SpecialtyCount	
	with sqlite3.connect('data/sqldata') as conn:
		df_sqlite_temp=read_db('select MemberID,Year,count(distinct(Specialty)) as SpecialtyCount from claims group by MemberID,Year;',con=conn)
	
	df_sqlite =	df_sqlite.merge(df_sqlite_temp,on=["MemberID","Year"])

	print('Specialty done...')
	
	# ProcedureGroupCount
	with sqlite3.connect('data/sqldata') as conn:
		df_sqlite_temp=read_db('select MemberID,Year,count(distinct(ProcedureGroup)) as ProcedureGroupCount from claims group by MemberID,Year;',con=conn)
	
	df_sqlite =	df_sqlite.merge(df_sqlite_temp,on=["MemberID","Year"])	

	print('ProcedureGroup done...')
	
	# PCPCount
	with sqlite3.connect('data/sqldata') as conn:
		df_sqlite_temp=read_db('select MemberID,Year,count(distinct(PCP)) as PCPCount from claims group by MemberID,Year;',con=conn)
	
	df_sqlite =	df_sqlite.merge(df_sqlite_temp,on=["MemberID","Year"])	
	
	print('PCP done...')
		
	#VendorCount
	with sqlite3.connect('data/sqldata') as conn:
		df_sqlite_temp=read_db('select MemberID,Year,count(distinct(Vendor)) as VendorCount from claims group by MemberID,Year;',con=conn)
	
	df_sqlite =	df_sqlite.merge(df_sqlite_temp,on=["MemberID","Year"])	
	
	print('Vendor done...')
	
	# ProviderIDCount
	with sqlite3.connect('data/sqldata') as conn:
		df_sqlite_temp=read_db('select MemberID,Year,count(distinct(ProviderID)) as ProviderIDCount from claims group by MemberID,Year;',con=conn)
		
	df_sqlite =	df_sqlite.merge(df_sqlite_temp,on=["MemberID","Year"])
	
	print('ProviderID done...')
	
	# Specialty x PlaceSvc
	with sqlite3.connect('data/sqldata') as conn:
		df_sqlite_temp=read_db('select MemberID,Year,count(*) as SpecialtyxPlaceSvc from (select distinct Specialty,PlaceSvc,MemberID,Year from claims) group by MemberID,Year;',con=conn)
	
	df_sqlite =	df_sqlite.merge(df_sqlite_temp,on=["MemberID","Year"])
	
	print('Specialty x PlaceSvc done...')

	return df_sqlite
