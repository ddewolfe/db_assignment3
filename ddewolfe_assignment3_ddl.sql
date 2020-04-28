# Dylan Dewolfe
# Assignment 3
## Create Tables / schema
##----------------------------------
CREATE TABLE Smokers(SS_NUM VARCHAR(18), Name VARCHAR(255),City VARCHAR(255), State VARCHAR(255), DOB DATE, PRIMARY KEY(SS_NUM));
CREATE TABLE COVID(SS_NUM INTEGER, case_confirmed_date DATE, Name VARCHAR(255), City VARCHAR(255), DOB DATE, PRIMARY KEY (SS_NUM));
CREATE TABLE Weather(Zip INTEGER, City VARCHAR(255), State VARCHAR(255), Temperature FLOAT,Humidity FLOAT, PRIMARY KEY(Zip));
CREATE TABLE FLU(SS_NUM VARCHAR(18), case_confirmed_date DATE, Name VARCHAR(255), City VARCHAR(255), DOB DATE, PRIMARY KEY (SS_NUM));
CREATE TABLE Population(num_pop INTEGER, City VARCHAR(255), State VARCHAR(255), Zip INTEGER, DateRecorded DATE, PRIMARY KEY (Zip));
## ----------------------------------

