#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
# Created by AndresD at 4/01/19

Features:
    + Add structure to ODM2 database to fit colombian government agencies data

@author:    Andres Felipe Duque Perez
Email:      andresfduque@gmail.com
"""

import os
import sys
import uuid
import argparse
import pandas as pd
from odm2api import models
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import numpy as np
from datetime import date
import os, calendar, pickle, time


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
# Start database session
# ======================================================================================================================
def start_db_session(connection_string):
    # Bind the engine to the metadata of the Base class so that the declarative can be accessed through a DBSession
    # instance
    try:
        engine = create_engine(connection_string, encoding='unicode_escape')
        models.setSchema(engine)
        db_session = sessionmaker(bind=engine)()
        # A DBSession() instance establishes all conversations with the database and represents a "staging zone" for all
        # the objects loaded into the database session object. Any change made against the objects in the session won't
        # be persisted into the database until you call session.commit(). If you're not happy about the changes, you can
        # revert all of them back to the last commit by calling session.rollback()
    except Exception as e:
        print(e)
        sys.exit(0)
    return db_session





# ======================================================================================================================
# Script Begin
# ======================================================================================================================
conn_string = args.conn_string
session = start_db_session(conn_string)

print("Loading ODM2COL structure using connection string: %s" % conn_string)

odm2_structure = os.path.dirname(os.getcwd()) + '/data/odm2col_structure/'


def ideam_txt_to_csv(source_folder, destiny_folder):
    # start timing
    startTime = time.time()

    # # Get first header line
    # f = open('/media/andres/ADATA AFDP/GE/GE/BD Hidrología/Caudal/00-Originales/TODAS IDEAM/ANDRESD1.txt', 'r')
    # first_line = f.readline()
    # first_line = ' '.join(first_line.split())
    # f.close()
    first_line = 'I D E A M - INSTITUTO DE HIDROLOGIA, METEOROLOGIA Y ESTUDIOS AMBIENTALES'

    # Define files to be read
    source_file_list = os.listdir(source_folder)
    source_file_list = [source_folder + i for i in source_file_list]

    # Create dictionary were data is going to be stored temporally]
    station_db = {}

    # Read each text file where data is stored and read each station header
    for files in source_file_list:
        f = open(files, 'r')
        i = 0
        code = 0
        years = []  # register period vector (in years)
        for line in f:

            # Get station basic parameters
            if i == 4:
                year = int(line[59:64])  # register year
                code1 = int(line[104:113])  # station code
                name = line[114:]  # station name
                name = ' '.join(name.split())
                years.append(year)

            if i == 6:  # get latitude and "departamento"
                lat = int(line[15:17]) + int(line[17:19]) / 60.
                lat_dir = line[20:22]  # latitude direction (North [N]; South [S])
                tipo = line[48:51]
                departamento = line[80:104]
                departamento = ' '.join(departamento.split())

                if lat_dir == 'S':  # correct value by latitude direction
                    lat = -lat

            if i == 7:  # get longitude and "municipio"
                lon = int(line[15:17]) + int(line[17:19]) / 60.
                lon = -lon
                municipio = line[80:104]
                municipio = ' '.join(municipio.split())

            if i == 8:  # get elevation and river name
                elevation = int(line[15:20])
                river_name = line[80:104]
                river_name = ' '.join(river_name.split())

            if i >= 8:
                # Create station in dictionary and add parameters
                if code != code1:
                    station_db[code1] = {'nombre': name, 'lat-lon': [lat, lon], 'municipio': municipio,
                                         'departamento': departamento, 'elevacion': elevation, 'corriente': river_name,
                                         'registros': [year], 'tipo': tipo, 'codigo': code1}

                    # add register period vector to station
                    if code != 0:
                        station_db[code]['registros'] = years[:-1]

                        # add empty timeseries - to be filled later in this script
                        first_year = station_db[code]['registros'][0]
                        last_year = station_db[code]['registros'][-1]
                        start_date = date(first_year, 1, 1)
                        end_date = date(last_year, 12, 31)
                        date_range_daily = pd.date_range(start_date, end_date)
                        date_range_monthly = pd.date_range(start_date, end_date, freq='M')
                        data_daily = np.zeros([len(date_range_daily)])
                        data_daily[:] = np.nan
                        data_max = np.zeros([len(date_range_monthly)])
                        data_max[:] = np.nan
                        data_min = np.zeros([len(date_range_monthly)])
                        data_min[:] = np.nan
                        station_timeseries_daily = pd.TimeSeries(data_daily, date_range_daily)
                        station_timeseries_max = pd.TimeSeries(data_max, date_range_monthly)
                        station_timeseries_min = pd.TimeSeries(data_min, date_range_monthly)
                        station_db[code]['caudales_diarios'] = station_timeseries_daily
                        station_db[code]['maximos_mensuales'] = station_timeseries_max
                        station_db[code]['minimos_mensuales'] = station_timeseries_min

                        years = [year]  # register period vector

                code = code1  # code pass for stations change

            # check if line is header (new year of data)
            if ' '.join(line.split()) == first_line:
                i = 0

            i += 1  # count

        # add data to the last station
        station_db[code]['registros'] = years  # period of register from last station
        first_year = station_db[code]['registros'][0]
        last_year = station_db[code]['registros'][-1]
        start_date = date(first_year, 1, 1)
        end_date = date(last_year, 12, 31)
        date_range_daily = pd.date_range(start_date, end_date)
        date_range_monthly = pd.date_range(start_date, end_date, freq='M')
        data_daily = np.zeros([len(date_range_daily)])
        data_daily[:] = np.nan
        data_max = np.zeros([len(date_range_monthly)])
        data_max[:] = np.nan
        data_min = np.zeros([len(date_range_monthly)])
        data_min[:] = np.nan
        station_timeseries_daily = pd.TimeSeries(data_daily, date_range_daily)
        station_timeseries_max = pd.TimeSeries(data_max, date_range_monthly)
        station_timeseries_min = pd.TimeSeries(data_min, date_range_monthly)
        station_db[code]['caudales_diarios'] = station_timeseries_daily
        station_db[code]['maximos_mensuales'] = station_timeseries_max
        station_db[code]['minimos_mensuales'] = station_timeseries_min

        f.close()

    # Read each text file where data is stored and read discharges data
    for files in source_file_list:
        f = open(files, 'r')
        i = 0
        for line in f:

            # Get station basic parameters
            if i == 4:
                year = int(line[59:64])  # register year
                code = int(line[104:113])  # station code

                j = 1

            if 14 <= i <= 44:
                day = int(line[11:13])
                day_line = split_ideam_line(line)  # month line of daily data

                # check leap year
                leap_year = calendar.isleap(year)
                if not leap_year:
                    if day <= 28:
                        station_db[code]['caudales_diarios'][date(year, 2, day)] = day_line[1]
                elif day <= 29:
                    station_db[code]['caudales_diarios'][date(year, 2, day)] = day_line[1]

                if day <= 30:
                    station_db[code]['caudales_diarios'][date(year, 1, day)] = day_line[0]
                    station_db[code]['caudales_diarios'][date(year, 3, day)] = day_line[2]
                    station_db[code]['caudales_diarios'][date(year, 4, day)] = day_line[3]
                    station_db[code]['caudales_diarios'][date(year, 5, day)] = day_line[4]
                    station_db[code]['caudales_diarios'][date(year, 6, day)] = day_line[5]
                    station_db[code]['caudales_diarios'][date(year, 7, day)] = day_line[6]
                    station_db[code]['caudales_diarios'][date(year, 8, day)] = day_line[7]
                    station_db[code]['caudales_diarios'][date(year, 9, day)] = day_line[8]
                    station_db[code]['caudales_diarios'][date(year, 10, day)] = day_line[9]
                    station_db[code]['caudales_diarios'][date(year, 11, day)] = day_line[10]
                    station_db[code]['caudales_diarios'][date(year, 12, day)] = day_line[11]
                else:
                    station_db[code]['caudales_diarios'][date(year, 1, day)] = day_line[0]
                    station_db[code]['caudales_diarios'][date(year, 3, day)] = day_line[2]
                    station_db[code]['caudales_diarios'][date(year, 5, day)] = day_line[4]
                    station_db[code]['caudales_diarios'][date(year, 7, day)] = day_line[6]
                    station_db[code]['caudales_diarios'][date(year, 8, day)] = day_line[7]
                    station_db[code]['caudales_diarios'][date(year, 10, day)] = day_line[9]
                    station_db[code]['caudales_diarios'][date(year, 12, day)] = day_line[11]

            if 47 == i:  # line where max discharge data is supposed to be located

                if line[0:3] == 'MAX':  # if "MAXIMO ABSOLUTO" exists
                    month_line = split_ideam_line(line)  # max daily instantaneous month flow
                    station_db[code]['maximos_mensuales'][date(year, 1, calendar.monthrange(year, 1)[1])] = month_line[0]
                    station_db[code]['maximos_mensuales'][date(year, 2, calendar.monthrange(year, 2)[1])] = month_line[1]
                    station_db[code]['maximos_mensuales'][date(year, 3, calendar.monthrange(year, 3)[1])] = month_line[2]
                    station_db[code]['maximos_mensuales'][date(year, 4, calendar.monthrange(year, 4)[1])] = month_line[3]
                    station_db[code]['maximos_mensuales'][date(year, 5, calendar.monthrange(year, 5)[1])] = month_line[4]
                    station_db[code]['maximos_mensuales'][date(year, 6, calendar.monthrange(year, 6)[1])] = month_line[5]
                    station_db[code]['maximos_mensuales'][date(year, 7, calendar.monthrange(year, 7)[1])] = month_line[6]
                    station_db[code]['maximos_mensuales'][date(year, 8, calendar.monthrange(year, 8)[1])] = month_line[7]
                    station_db[code]['maximos_mensuales'][date(year, 9, calendar.monthrange(year, 9)[1])] = month_line[8]
                    station_db[code]['maximos_mensuales'][date(year, 10, calendar.monthrange(year, 10)[1])] = month_line[9]
                    station_db[code]['maximos_mensuales'][date(year, 11, calendar.monthrange(year, 11)[1])] = month_line[10]
                    station_db[code]['maximos_mensuales'][date(year, 12, calendar.monthrange(year, 12)[1])] = month_line[11]
                elif line[0:3] == 'MIN':  # if "MINIMA MEDIA" exists:
                    month_line = split_ideam_line(line)  # min daily instantaneous month flow
                    station_db[code]['minimos_mensuales'][date(year, 1, calendar.monthrange(year, 1)[1])] = month_line[0]
                    station_db[code]['minimos_mensuales'][date(year, 2, calendar.monthrange(year, 2)[1])] = month_line[1]
                    station_db[code]['minimos_mensuales'][date(year, 3, calendar.monthrange(year, 3)[1])] = month_line[2]
                    station_db[code]['minimos_mensuales'][date(year, 4, calendar.monthrange(year, 4)[1])] = month_line[3]
                    station_db[code]['minimos_mensuales'][date(year, 5, calendar.monthrange(year, 5)[1])] = month_line[4]
                    station_db[code]['minimos_mensuales'][date(year, 6, calendar.monthrange(year, 6)[1])] = month_line[5]
                    station_db[code]['minimos_mensuales'][date(year, 7, calendar.monthrange(year, 7)[1])] = month_line[6]
                    station_db[code]['minimos_mensuales'][date(year, 8, calendar.monthrange(year, 8)[1])] = month_line[7]
                    station_db[code]['minimos_mensuales'][date(year, 9, calendar.monthrange(year, 9)[1])] = month_line[8]
                    station_db[code]['minimos_mensuales'][date(year, 10, calendar.monthrange(year, 10)[1])] = month_line[9]
                    station_db[code]['minimos_mensuales'][date(year, 11, calendar.monthrange(year, 11)[1])] = month_line[10]
                    station_db[code]['minimos_mensuales'][date(year, 12, calendar.monthrange(year, 12)[1])] = month_line[11]

            if 48 == i:  # line where min discharge data is suposed to be located
                if line[0:3] == 'MIN':  # if "MINIMA MEDIA" exists:
                    month_line = split_ideam_line(line)  # min daily instantaneous month flow
                    station_db[code]['minimos_mensuales'][date(year, 1, calendar.monthrange(year, 1)[1])] = month_line[0]
                    station_db[code]['minimos_mensuales'][date(year, 2, calendar.monthrange(year, 2)[1])] = month_line[1]
                    station_db[code]['minimos_mensuales'][date(year, 3, calendar.monthrange(year, 3)[1])] = month_line[2]
                    station_db[code]['minimos_mensuales'][date(year, 4, calendar.monthrange(year, 4)[1])] = month_line[3]
                    station_db[code]['minimos_mensuales'][date(year, 5, calendar.monthrange(year, 5)[1])] = month_line[4]
                    station_db[code]['minimos_mensuales'][date(year, 6, calendar.monthrange(year, 6)[1])] = month_line[5]
                    station_db[code]['minimos_mensuales'][date(year, 7, calendar.monthrange(year, 7)[1])] = month_line[6]
                    station_db[code]['minimos_mensuales'][date(year, 8, calendar.monthrange(year, 8)[1])] = month_line[7]
                    station_db[code]['minimos_mensuales'][date(year, 9, calendar.monthrange(year, 9)[1])] = month_line[8]
                    station_db[code]['minimos_mensuales'][date(year, 10, calendar.monthrange(year, 10)[1])] = month_line[9]
                    station_db[code]['minimos_mensuales'][date(year, 11, calendar.monthrange(year, 11)[1])] = month_line[10]
                    station_db[code]['minimos_mensuales'][date(year, 12, calendar.monthrange(year, 12)[1])] = month_line[11]

                    # check if line is header (new year of data)
            if ' '.join(line.split()) == first_line:
                i = 0

            i += 1  # count

        f.close()

    output_path = destiny_folder
    pickle.dump(station_db, open(output_path, 'w'))

    print
    ''
    print
    '*********************************************************************'
    print
    '!Discharges database succesfully created!'

    # figure out how long the script took to run
    endTime = time.time()

    print
    'Execution time: ' + str(round(endTime - startTime, 1)) + ' seconds'
    print
    '*********************************************************************\n'
