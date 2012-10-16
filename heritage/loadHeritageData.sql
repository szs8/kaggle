-- Import data in Table drug
create table drug (MemberId INTEGER, Year TEXT, DSFS TEXT, DrugCount TEXT);
.separator ','
.import 'data/DrugCount.csv' drug

-- Import data in Table lab
create table lab (MemberId INTEGER, Year TEXT, DSFS TEXT, LabCount TEXT);
.separator ','
.import 'data/LabCount.csv' lab

-- Import data in Table claims
create table claims (MemberID INTEGER, ProviderID INTEGER, Vendor INTEGER, PCP INTEGER, Year TEXT,
 Specialty TEXT, PlaceSvc TEXT, PayDelay TEXT, LengthOfStay TEXT, DSFS TEXT, PrimaryConditionGroup TEXT,
  CharlsonIndex TEXT, ProcedureGroup TEXT, SupLOS TEXT);
.separator ','
.import 'data/Claims.csv' claims

-- Import data in Table members
create table members (MemberID INTEGER, AgeAtFirstClaim TEXT,Sex TEXT);
.separator ','
.import 'data/Members.csv' members

-- Import data in Table daysY2
create table daysY2 (MemberID INTEGER, ClaimsTruncated INTEGER,DaysInHospital INTEGER);
.separator ','
.import 'data/DaysInHospital_Y2.csv' daysY2

-- Import data in Table daysY3
create table daysY3 (MemberID INTEGER, ClaimsTruncated INTEGER,DaysInHospital INTEGER);
.separator ','
.import 'data/DaysInHospital_Y3.csv' daysY3

-- Import data in Table target
create table target (MemberID INTEGER, ClaimsTruncated INTEGER,DaysInHospital INTEGER);
.separator ','
.import 'data/Target.csv' target

