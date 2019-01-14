#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
# Created by AndresD at 5/01/19

Features: 
    + Add ODM2COL base data (streamflow and precipitation)

@author:    Andres Felipe Duque Perez
Email:      andresfduque@gmail.com
"""

import os
import sys
import uuid
import argparse
import  psycopg2
import pandas as pd
from odm2api import models
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


# ======================================================================================================================
# handles customizing the error messages from ArgParse
# ======================================================================================================================
class MyParser(argparse.ArgumentParser):
        def error(self, message):
            sys.stderr.write("------------------------------\n")
            sys.stderr.write('error: %s\n' % message)
            sys.stderr.write("------------------------------\n")
            self.print_help()
            sys.exit(2)


# handle argument parsing
info = "A simple script that loads up ODM2COL basic structure into the ODM2 database"
parser = MyParser(description=info, add_help=True)
parser.add_argument(
        help="Format: {engine}+{driver}://{user}:{pass}@{address}/{db}\n"
        "mysql+pymysql://ODM:odm@localhost/odm2\n"
        "mssql+pyodbc://ODM:123@localhost/odm2\n"
        "postgresql+psycopg2://postgres:postgres@localhost/odm2col\n"
        "sqlite+pysqlite:///path/to/file",
        default=True, type=str, dest='conn_string')
parser.add_argument('-d', '--debug', help="Debugging program without committing anything to remote database",
                    action="store_true")
args = parser.parse_args()

# ======================================================================================================================
# Script Begin
# ======================================================================================================================
# Verify connection string
engine = None
session = None
conn_string = args.conn_string
# conn_string = "postgresql+psycopg2://postgres:postgres@localhost/odm2col"
try:
    engine = create_engine(conn_string, encoding='unicode_escape')
    models.setSchema(engine)
    session = sessionmaker(bind=engine)()
except Exception as e:
    print(e)
    sys.exit(0)

print("Loading ODM2COL structure using connection string: %s" % conn_string)

odm2_structure = os.path.dirname(os.getcwd()) + '/data/odm2col_structure/'


# ======================================================================================================================
# Create Timeseries Results
# ======================================================================================================================
conn = psycopg2.connect(dbname='odm2col', host='localhost', user='postgres', password='postgres')
feature_actions_df = pd.read_sql('select * from odm2.featureactions;', conn)
sampling_features_df = pd.read_sql('select * from odm2.samplingfeatures;', conn)
sampling_feature_codes = sampling_features_df.samplingfeaturecode

precipitation_data_folder = '/SHDD/Digital Library/05-Hydrology/03-Timeseries/01-Precipitation/01-IDEAM/02-Preprocessed\
/Total Diaria/'
stream_flow_data_folder = '/SHDD/Digital Library/05-Hydrology/03-Timeseries/02-Streamflow/01-IDEAM/02-Preprocessed/'

# get base data to determine the results from base data list, corresponding each variable associated to IDEAM monitoring
# sites. The sampling feature associated with the feature action corresponds to station code]
precipitation_results_list = os.listdir(precipitation_data_folder)
precipitation_results_list = [i.split('.')[0] for i in precipitation_results_list]
stream_flow_results_list = os.listdir(stream_flow_data_folder + 'Daily Means')
stream_flow_results_list = [i.split('.')[0] for i in stream_flow_results_list]

base_data_folder = pd.read_csv(odm2_structure + 'People/people.csv')
objects_people = []
for i in sampling_feature_codes:
    try:
        obj_person = None
        obj_person = models.People()
        obj_person.PersonID = str(people_csv['PersonID'][i])
        obj_person.PersonFirstName = people_csv['PersonFirstName'][i]
        if not pd.isna(people_csv['PersonMiddleName'][i]):
            obj_person.PersonMiddleName = people_csv['PersonMiddleName'][i]
        obj_person.PersonLastName = people_csv['PersonLastName'][i]
        objects_people.append(obj_person)
    except Exception as e:
        session.rollback()
        if obj_person.PersonID is not None:
            print("issue loading single object %s: %s " % (obj_person.PersonID, e))
        pass
session.add_all(objects_people)
if not args.debug:
    session.commit()