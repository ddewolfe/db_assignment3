# Dylan DeWolfe
# dewol103@mail.chapman.edu
# CPSC 408
# Assignment 3 Faker Testing

import mysql
import mysql.connector
from faker import Faker
import csv
import sys
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from pandas import DataFrame

# -----------------------------------
# connection stuff
my_db = mysql.connector.connect(
  host="34.106.102.239",
  user="root",
  password="2421",
  database="ASSIGNMENT_3"
)

cursor = my_db.cursor()  # allows python to execute sql statement

# testing connection to the database

# -------------------------------------
# Faker testing and such
fake = Faker()
# need fake: ssn, Name, location, age, date, zip code, latitude, longitude, temperature, humidity, weather description,
# population number
# writer.writerow([fake.name(),fake.street_address(), fake.city(), fake.state(), fake.phone_number, fake.ssn])
# ------------------------------------------
# start fake data generation /////////////////////////////////
# for smokers table:
#csv_file = open('smokers.csv', 'w')
#writer = csv.writer(csv_file)
#for x in range(0,10):
#writer.writerow([fake.ssn(), fake.name(), fake.city(), fake.state(), fake.date_of_birth()])
# -------------------------------------------
# for COVID Table
# SS_NUM INTEGER, Name VARCHAR(255),Location VARCHAR(255),Age INTEGER,Date DATE, PRIMARY KEY (SS_NUM)
#csv_fileCovid = open('COVID.csv', 'w')
#writer = csv.writer(csv_fileCovid)
#for x in range(0,10):
#    writer.writerow([fake.ssn(), fake.date(), fake.name(), fake.city(), fake.state(), fake.date_of_birth()])
# -------------------------------------------
# for Weather Table
# (Zip INTEGER, Latitude FLOAT, Longitude FLOAT,Temperature FLOAT,Humidity
# FLOAT,Description VARCHAR(255),PRIMARY KEY(Zip))
#csv_fileWeather = open('Weather.csv', 'w')
#writer = csv.writer(csv_fileWeather)
#for x in range(0,10):
#    writer.writerow([fake.postalcode(), fake.latitude(), fake.longitude(), fake.city(), fake.state(), fake.pyfloat(left_digits=None, right_digits=2, positive=True, min_value=40, max_value=110),
#                     fake.pyfloat(left_digits=None, right_digits=2, positive=True, min_value=0, max_value=70)])

# -------------------------------------------
# For FLU table
# (SS_NUM INTEGER, Name VARCHAR(255),Location VARCHAR(255),Age INTEGER,Date DATE, PRIMARY KEY (SS_NUM));
#csv_fileFlu = open('flu.csv', 'w')
#writer = csv.writer(csv_fileFlu)
#for x in range(0,10):
#    writer.writerow([fake.ssn(), fake.date(), fake.name(), fake.city(), fake.state(), fake.date_of_birth()])
# -------------------------------------------
# For Population Table
# (num_pop INTEGER,Location VARCHAR(255),Zip INTEGER,PRIMARY KEY (Zip));
#csv_filePop = open('Population.csv', 'w')
#writer = csv.writer(csv_filePop)
#for x in range(0,10):
#    writer.writerow([fake.pyint(min_value = 100000, max_value = 3000000), fake.city(), fake.state(), fake.postalcode(), ])
# End Fake data Generation In seperate CSVS //////////////////////////////////
# -------------------------------------------
# Make command line parameter args and setting up command line args

#arguments = len(sys.argv) -1
#if len(sys.argv) == 2:
#    csv_file_name = sys.argv[1]
#    num_tuples = sys.argv[2]
# --------------------------------------------

# using sys module and merging into one CSV and creating a function to call when command line parameters are entered

# faker usage
def auto_csv():

    csv_file_name = sys.argv[1]
    num_tuples = int(sys.argv[2])

    csv_file_arg = open(csv_file_name, 'w')
    writer1 = csv.writer(csv_file_arg)
    # adding header for slicing purposes
    writer1.writerow(["Header1", "Header2", "Header3", "Header4", "Header5"])
    for x in range(0, num_tuples):
        writer1.writerow([fake.ssn(), fake.name(), fake.city(), fake.state(), fake.date_of_birth()])

    for i in range(0, num_tuples):
        writer1.writerow([fake.ssn(), fake.date(), fake.name(), fake.city(), fake.date_of_birth()])

    for y in range(0, num_tuples):
        writer1.writerow([fake.postalcode(), fake.city(), fake.state(),
                         fake.pyfloat(left_digits=None, right_digits=2, positive=True, min_value=40, max_value=110),
                         fake.pyfloat(left_digits=None, right_digits=2, positive=True, min_value=0, max_value=70)])
    for z in range(0, num_tuples):
        writer1.writerow([fake.ssn(), fake.date(), fake.name(), fake.city(), fake.date_of_birth()])
    for d in range(0, num_tuples):
        writer1.writerow([fake.pyint(min_value=100000, max_value=3000000), fake.city(), fake.state(), fake.postalcode(),
                          fake.date()])

# -------------------------------------------
# calling my auto_csv to write to file.

auto_csv()

# working Check... DONE

# ---------------------------------------------
# need to export to CSV and Import to DB still
# maybe use pandas here
# pandas df to sql
# entries up to num_tuples should be in 1 df and then the next should be in a
# seperate dataframe to make it easier to import into my database.....

# converting to csv and updating database
def csv_to_df():
    # Still need to change hard coded slices to use variables, for any user input from cmd line
    csv_file_name_df = sys.argv[1]
    smokers_rows = int(sys.argv[2])
    covid_rows = int(sys.argv[2])*2
    weather_rows = int(sys.argv[2])*3
    flu_rows = int(sys.argv[2])*4
    pop_rows = int(sys.argv[2])*5
    # csv_df = pd.read_csv(csv_file_name_df)
    csv_df = pd.read_csv(csv_file_name_df)
    # print(csv_df)
    smokers_df = csv_df[:smokers_rows]
    smokers_df.rename(columns={
        'Header1': 'SS_NUM', 'Header2': 'Name', 'Header3': 'City', 'Header4': 'State', 'Header5': 'DOB'}, inplace=True)
    print(smokers_df)
    covid_df = csv_df[smokers_rows:covid_rows] # 8 [smokers_rows:8]
    covid_df.rename(columns={
        'Header1': 'SS_NUM', 'Header2': 'case_confirmed_date', 'Header3': 'Name', 'Header4': 'City', 'Header5': 'DOB'},
        inplace=True)
    print(covid_df)
    weather_df = csv_df[covid_rows:weather_rows] # [9:13]
    weather_df.rename(columns={
        'Header1': 'Zip', 'Header2': 'City', 'Header3': 'State', 'Header4': 'Temperature', 'Header5': 'Humidity'},
        inplace=True)
    print(weather_df)
    flu_df = csv_df[weather_rows:flu_rows]  # [13:17]
    flu_df.rename(columns={
        'Header1': 'SS_NUM', 'Header2': 'case_confirmed_date', 'Header3': 'Name', 'Header4': 'City', 'Header5': 'DOB'},
        inplace=True)
    print(flu_df)
    pop_df = csv_df[flu_rows:pop_rows]  # [17:21]
    pop_df.rename(columns={
        'Header1': 'num_pop', 'Header2': 'City', 'Header3': 'State', 'Header4': 'Zip', 'Header5': 'DateRecorded'},
        inplace=True)
    print(pop_df)

    # smokers_df.to_sql('Smokers', con=my_db, index=False, if_exists='append') // this apparently only works for sqlite
    # inserting each line of the pandas dataframe iteratively....
    # For smokers table --------------------
    cols = "`,`".join([str(i) for i in smokers_df.columns.tolist()])
    for i, row in smokers_df.iterrows():
        test_sql = "INSERT INTO `Smokers` (`" + cols + "`) VALUES (" + "%s," * (len(row) - 1) + "%s)"
        cursor.execute(test_sql, tuple(row))
        my_db.commit()


     # For covid table ----------------------------
    cols2 = "`,`".join([str(i) for i in covid_df.columns.tolist()])

    for i, row in covid_df.iterrows():
        test_sql2 = "INSERT INTO `COVID` (`" + cols2 + "`) VALUES (" + "%s," * (len(row) - 1) + "%s)"
        cursor.execute(test_sql2, tuple(row))
        my_db.commit()



    # for population table ------------------------
    cols4 = "`,`".join([str(i) for i in pop_df.columns.tolist()])

    for i, row in pop_df.iterrows():
        test_sql4 = "INSERT INTO `Population` (`" + cols4 + "`) VALUES (" + "%s," * (len(row) - 1) + "%s)"
        cursor.execute(test_sql4, tuple(row))
        my_db.commit()

    # for weather table----------------------------
    cols3 = "`,`".join([str(i) for i in weather_df.columns.tolist()])

    for i, row in weather_df.iterrows():
        test_sql3 = "INSERT INTO `Weather` (`" + cols3 + "`) VALUES (" + "%s," * (len(row) - 1) + "%s)"
        cursor.execute(test_sql3, tuple(row))
        my_db.commit()

    # for FLU Table
    cols5 = "`,`".join([str(i) for i in flu_df.columns.tolist()])

    for i, row in flu_df.iterrows():
        test_sql5 = "INSERT INTO `FLU` (`" + cols5 + "`) VALUES (" + "%s," * (len(row) - 1) + "%s)"
        cursor.execute(test_sql5, tuple(row))
        my_db.commit()

# need to rename columns for each df... to match my table schema..
# ["Header1", "Header2", "Header3", "Header4", "Header5"]
csv_to_df()

# --------------------------------------------
# the order of the tables is: smokers, covid, weather, FLU, Population

# testing to see if i can even connect pandas to my sql db rn...

#df_sql_test = df = pd.read_sql('Smokers', my_db, columns= ['SS_NUM', 'Name', 'City', 'State', 'DOB'])
# just Testing the connection to server
# sql_test = "INSERT INTO Smokers(SS_NUM,Name,City,State,DOB) VALUES (%s,%s,%s,%s,%s);"
# vals = ('127-46-5405', ' Raymond Murphy', 'Lake Charlesfurt',  'Illinois', '1970-08-09')
# cursor.execute(sql_test, vals)
# my_db.commit()









