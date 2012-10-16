'''
Load Heritage dataset into sqlite3 database
Needs
1. sqlite3 (http://www.sqlite.org/download.html)
2. excellent sqlite3 python library (http://docs.python.org/library/sqlite3.html)
'''
import os
os.system("sqlite3 data/sqldata < loadHeritageData.sql")