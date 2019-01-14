#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
# Created by AndresD at 4/01/19

Features: 
    + Enter feature name and description here

@author:    Andres Felipe Duque Perez
Email:      andresfduque@gmail.com
"""

import numpy as np
import pandas as pd
from datetime import date
import os, calendar, pickle, time


def split_ideam_line(ideam_line):
    """
    Split daily discharge data by columns

        ideam_line: line containing data in IDEAM format

    For IDEAM's text file the position of each data value is:
        JAN: (18, 26)
        FEB: (27, 35)
        MAR: (36, 44)
        APR: (45, 53)
        MAY: (54, 62)
        JUN: (63, 71)
        JUL: (72, 80)
        AUG: (81, 89)
        SEP: (90, 98)
        OCT: (99, 107)
        NOV: (108, 116)
        DIC: (117, 125)
    """
    position_index = [(18, 26), (27, 35), (36, 44), (45, 53), (54, 62), (63, 71), (72, 80), (81, 89), (90, 98),
                      (99, 107), (108, 116), (117, 125)]

    # data vector
    nan_tuple = (np.nan, np.nan)
    vector = [nan_tuple, nan_tuple, nan_tuple, nan_tuple, nan_tuple, nan_tuple,
              nan_tuple, nan_tuple, nan_tuple, nan_tuple, nan_tuple, nan_tuple]

    # read each month data [for each day]
    j = 0
    for i in position_index:
        data = np.nan
        data_quality = np.nan
        if not ideam_line[i[0]:i[1]] == '':
            try:
                data = float(ideam_line[i[0]:i[1]])
                if str.isnumeric(ideam_line[i[1]:i[1] + 1]):
                    data_quality = int(ideam_line[i[1]:i[1] + 1])
                else:
                    data_quality = np.nan
            except ValueError:
                # print('error')
                data = np.nan
        else:
            data = np.nan
            data_quality = np.nan

        vector[j] = (data, data_quality)
        j += 1

    return vector


# start timing
startTime = time.time()

# Get first header line
f = open('/SHDD/Digital Library/05-Hydrology/03-Timeseries/02-Streamflow/01-IDEAM/01-Original/ANDRESD1.txt', 'r')
first_line = f.readline()
first_line = ' '.join(first_line.split())
f.close()

# Define files to be read
source_folder = '/SHDD/Digital Library/05-Hydrology/03-Timeseries/02-Streamflow/01-IDEAM/01-Original/'
source_file_list = os.listdir(source_folder)
source_file_list = [source_folder + i for i in source_file_list]

# Create database [dictionary] were data is going to be stored]
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
            year = int(line[59:64])     # register year
            code1 = int(line[104:113])  # station code
            name = line[114:]           # station name
            name = ' '.join(name.split())
            years.append(year)

        if i == 6:  # get latitude and "departamento"
            lat = int(line[15:17]) + int(line[17:19])/60.
            lat_dir = line[20:22]   # latitude direction (North [N]; South [S])
            tipo = line[48:51]
            departamento = line[80:104]
            departamento = ' '.join(departamento.split())

            if lat_dir == 'S':    # correct value by latitude direction
                lat = -lat

        if i == 7:  # get longitude and "municipio"
            lon = int(line[15:17]) + int(line[17:19])/60.
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
                station_db[code1] = {'nombre': name,
                                     'lat-lon': [lat, lon], 'municipio': municipio,
                                     'departamento': departamento, 'elevacion': elevation,
                                     'corriente': river_name, 'registros': [year], 'tipo': tipo,
                                     'codigo': code1}

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
                    data_daily_quality = np.zeros([len(date_range_daily)])
                    data_daily_quality[:] = np.nan
                    data_max = np.zeros([len(date_range_monthly)])
                    data_max[:] = np.nan
                    data_max_quality = np.zeros([len(date_range_monthly)])
                    data_max_quality[:] = np.nan
                    data_min = np.zeros([len(date_range_monthly)])
                    data_min[:] = np.nan
                    data_min_quality = np.zeros([len(date_range_monthly)])
                    data_min_quality[:] = np.nan

                    station_timeseries_daily = pd.Series(data_daily, date_range_daily)
                    station_timeseries_max = pd.Series(data_max, date_range_monthly)
                    station_timeseries_min = pd.Series(data_min, date_range_monthly)

                    station_timeseries_daily_quality = pd.Series(data_daily_quality, date_range_daily)
                    station_timeseries_max_quality = pd.Series(data_max_quality, date_range_monthly)
                    station_timeseries_min_quality = pd.Series(data_min_quality, date_range_monthly)

                    station_db[code]['caudales_diarios'] = station_timeseries_daily
                    station_db[code]['maximos_mensuales'] = station_timeseries_max
                    station_db[code]['minimos_mensuales'] = station_timeseries_min

                    station_db[code]['caudales_diarios_quality'] = station_timeseries_daily_quality
                    station_db[code]['maximos_mensuales_quality'] = station_timeseries_max_quality
                    station_db[code]['minimos_mensuales_quality'] = station_timeseries_min_quality

                    years = [year]  # register period vector

            code = code1    # code pass for stations change

        # check if line is header (new year of data)
        if ' '.join(line.split()) == first_line:
                i = 0

        i += 1   # count

    # add data to the last station
    station_db[code]['registros'] = years   # period of register from last station
    first_year = station_db[code]['registros'][0]
    last_year = station_db[code]['registros'][-1]
    start_date = date(first_year, 1, 1)
    end_date = date(last_year, 12, 31)
    date_range_daily = pd.date_range(start_date, end_date)
    date_range_monthly = pd.date_range(start_date, end_date, freq='M')
    data_daily = np.zeros([len(date_range_daily)])
    data_daily[:] = np.nan
    data_daily_quality = np.zeros([len(date_range_daily)])
    data_daily_quality[:] = np.nan
    data_max = np.zeros([len(date_range_monthly)])
    data_max[:] = np.nan
    data_max_quality = np.zeros([len(date_range_monthly)])
    data_max_quality[:] = np.nan
    data_min = np.zeros([len(date_range_monthly)])
    data_min[:] = np.nan
    data_min_quality = np.zeros([len(date_range_monthly)])
    data_min_quality[:] = np.nan

    station_timeseries_daily = pd.Series(data_daily, date_range_daily)
    station_timeseries_max = pd.Series(data_max, date_range_monthly)
    station_timeseries_min = pd.Series(data_min, date_range_monthly)

    station_timeseries_daily_quality = pd.Series(data_daily_quality, date_range_daily)
    station_timeseries_max_quality = pd.Series(data_max_quality, date_range_monthly)
    station_timeseries_min_quality = pd.Series(data_min_quality, date_range_monthly)

    station_db[code]['caudales_diarios'] = station_timeseries_daily
    station_db[code]['maximos_mensuales'] = station_timeseries_max
    station_db[code]['minimos_mensuales'] = station_timeseries_min

    station_db[code]['caudales_diarios_quality'] = station_timeseries_daily_quality
    station_db[code]['maximos_mensuales_quality'] = station_timeseries_max_quality
    station_db[code]['minimos_mensuales_quality'] = station_timeseries_min_quality

    f.close()

# Read each text file where data is stored and read discharges data
for files in source_file_list:
    f = open(files, 'r')
    i = 0
    for line in f:

        # Get station basic parameters
        if i == 4:
            year = int(line[59:64])     # register year
            code = int(line[104:113])   # station code

            j = 1

        if 14 <= i <= 44:
            day = int(line[11:13])
            day_line_complete = split_ideam_line(line)
            day_line = [i[0] for i in day_line_complete]
            day_line_quality = [i[1] for i in day_line_complete]

            # check leap year
            bisiesto = calendar.isleap(year)
            if not bisiesto:
                if day <= 28:
                    station_db[code]['caudales_diarios'][date(year, 2, day)] = day_line[1]
                    station_db[code]['caudales_diarios_quality'][date(year, 2, day)] = day_line_quality[1]
            elif day <= 29:
                station_db[code]['caudales_diarios'][date(year, 2, day)] = day_line[1]
                station_db[code]['caudales_diarios_quality'][date(year, 2, day)] = day_line_quality[1]

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

                station_db[code]['caudales_diarios_quality'][date(year, 1, day)] = day_line_quality[0]
                station_db[code]['caudales_diarios_quality'][date(year, 3, day)] = day_line_quality[2]
                station_db[code]['caudales_diarios_quality'][date(year, 4, day)] = day_line_quality[3]
                station_db[code]['caudales_diarios_quality'][date(year, 5, day)] = day_line_quality[4]
                station_db[code]['caudales_diarios_quality'][date(year, 6, day)] = day_line_quality[5]
                station_db[code]['caudales_diarios_quality'][date(year, 7, day)] = day_line_quality[6]
                station_db[code]['caudales_diarios_quality'][date(year, 8, day)] = day_line_quality[7]
                station_db[code]['caudales_diarios_quality'][date(year, 9, day)] = day_line_quality[8]
                station_db[code]['caudales_diarios_quality'][date(year, 10, day)] = day_line_quality[9]
                station_db[code]['caudales_diarios_quality'][date(year, 11, day)] = day_line_quality[10]
                station_db[code]['caudales_diarios_quality'][date(year, 12, day)] = day_line_quality[11]
            else:
                station_db[code]['caudales_diarios'][date(year, 1, day)] = day_line[0]
                station_db[code]['caudales_diarios'][date(year, 3, day)] = day_line[2]
                station_db[code]['caudales_diarios'][date(year, 5, day)] = day_line[4]
                station_db[code]['caudales_diarios'][date(year, 7, day)] = day_line[6]
                station_db[code]['caudales_diarios'][date(year, 8, day)] = day_line[7]
                station_db[code]['caudales_diarios'][date(year, 10, day)] = day_line[9]
                station_db[code]['caudales_diarios'][date(year, 12, day)] = day_line[11]

                station_db[code]['caudales_diarios_quality'][date(year, 1, day)] = day_line_quality[0]
                station_db[code]['caudales_diarios_quality'][date(year, 3, day)] = day_line_quality[2]
                station_db[code]['caudales_diarios_quality'][date(year, 5, day)] = day_line_quality[4]
                station_db[code]['caudales_diarios_quality'][date(year, 7, day)] = day_line_quality[6]
                station_db[code]['caudales_diarios_quality'][date(year, 8, day)] = day_line_quality[7]
                station_db[code]['caudales_diarios_quality'][date(year, 10, day)] = day_line_quality[9]
                station_db[code]['caudales_diarios_quality'][date(year, 12, day)] = day_line_quality[11]

        if 47 == i:     # line where max discharge data is supposed to be located

            if line[0:3] == 'MAX':  # if "MAXIMO ABSOLUTO" exists
                month_line = split_ideam_line(line)  # max monthly instantaneous month flow
                month_line_complete = split_ideam_line(line)
                month_line = [i[0] for i in month_line_complete]
                month_line_quality = [i[1] for i in month_line_complete]

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

                station_db[code]['maximos_mensuales_quality'][date(year, 1, calendar.monthrange(year, 1)[1])] = month_line_quality[0]
                station_db[code]['maximos_mensuales_quality'][date(year, 2, calendar.monthrange(year, 2)[1])] = month_line_quality[1]
                station_db[code]['maximos_mensuales_quality'][date(year, 3, calendar.monthrange(year, 3)[1])] = month_line_quality[2]
                station_db[code]['maximos_mensuales_quality'][date(year, 4, calendar.monthrange(year, 4)[1])] = month_line_quality[3]
                station_db[code]['maximos_mensuales_quality'][date(year, 5, calendar.monthrange(year, 5)[1])] = month_line_quality[4]
                station_db[code]['maximos_mensuales_quality'][date(year, 6, calendar.monthrange(year, 6)[1])] = month_line_quality[5]
                station_db[code]['maximos_mensuales_quality'][date(year, 7, calendar.monthrange(year, 7)[1])] = month_line_quality[6]
                station_db[code]['maximos_mensuales_quality'][date(year, 8, calendar.monthrange(year, 8)[1])] = month_line_quality[7]
                station_db[code]['maximos_mensuales_quality'][date(year, 9, calendar.monthrange(year, 9)[1])] = month_line_quality[8]
                station_db[code]['maximos_mensuales_quality'][date(year, 10, calendar.monthrange(year, 10)[1])] = month_line_quality[9]
                station_db[code]['maximos_mensuales_quality'][date(year, 11, calendar.monthrange(year, 11)[1])] = month_line_quality[10]
                station_db[code]['maximos_mensuales_quality'][date(year, 12, calendar.monthrange(year, 12)[1])] = month_line_quality[11]

            elif line[0:3] == 'MIN':    # if "MINIMA MEDIA" exists:
                month_line = split_ideam_line(line)  # min monthly instantaneous month flow
                month_line_complete = split_ideam_line(line)
                month_line = [i[0] for i in month_line_complete]
                month_line_quality = [i[1] for i in month_line_complete]

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

                station_db[code]['minimos_mensuales_quality'][date(year, 1, calendar.monthrange(year, 1)[1])] = month_line_quality[0]
                station_db[code]['minimos_mensuales_quality'][date(year, 2, calendar.monthrange(year, 2)[1])] = month_line_quality[1]
                station_db[code]['minimos_mensuales_quality'][date(year, 3, calendar.monthrange(year, 3)[1])] = month_line_quality[2]
                station_db[code]['minimos_mensuales_quality'][date(year, 4, calendar.monthrange(year, 4)[1])] = month_line_quality[3]
                station_db[code]['minimos_mensuales_quality'][date(year, 5, calendar.monthrange(year, 5)[1])] = month_line_quality[4]
                station_db[code]['minimos_mensuales_quality'][date(year, 6, calendar.monthrange(year, 6)[1])] = month_line_quality[5]
                station_db[code]['minimos_mensuales_quality'][date(year, 7, calendar.monthrange(year, 7)[1])] = month_line_quality[6]
                station_db[code]['minimos_mensuales_quality'][date(year, 8, calendar.monthrange(year, 8)[1])] = month_line_quality[7]
                station_db[code]['minimos_mensuales_quality'][date(year, 9, calendar.monthrange(year, 9)[1])] = month_line_quality[8]
                station_db[code]['minimos_mensuales_quality'][date(year, 10, calendar.monthrange(year, 10)[1])] = month_line_quality[9]
                station_db[code]['minimos_mensuales_quality'][date(year, 11, calendar.monthrange(year, 11)[1])] = month_line_quality[10]
                station_db[code]['minimos_mensuales_quality'][date(year, 12, calendar.monthrange(year, 12)[1])] = month_line_quality[11]

        if 48 == i: # line where min discharge data is suposed to be located
            if line[0:3] == 'MIN':  # if "MINIMA MEDIA" exists:
                month_line = split_ideam_line(line)  # min monthly instantaneous month flow
                month_line_complete = split_ideam_line(line)
                month_line = [i[0] for i in month_line_complete]
                month_line_quality = [i[1] for i in month_line_complete]

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

                station_db[code]['minimos_mensuales_quality'][date(year, 1, calendar.monthrange(year, 1)[1])] = month_line_quality[0]
                station_db[code]['minimos_mensuales_quality'][date(year, 2, calendar.monthrange(year, 2)[1])] = month_line_quality[1]
                station_db[code]['minimos_mensuales_quality'][date(year, 3, calendar.monthrange(year, 3)[1])] = month_line_quality[2]
                station_db[code]['minimos_mensuales_quality'][date(year, 4, calendar.monthrange(year, 4)[1])] = month_line_quality[3]
                station_db[code]['minimos_mensuales_quality'][date(year, 5, calendar.monthrange(year, 5)[1])] = month_line_quality[4]
                station_db[code]['minimos_mensuales_quality'][date(year, 6, calendar.monthrange(year, 6)[1])] = month_line_quality[5]
                station_db[code]['minimos_mensuales_quality'][date(year, 7, calendar.monthrange(year, 7)[1])] = month_line_quality[6]
                station_db[code]['minimos_mensuales_quality'][date(year, 8, calendar.monthrange(year, 8)[1])] = month_line_quality[7]
                station_db[code]['minimos_mensuales_quality'][date(year, 9, calendar.monthrange(year, 9)[1])] = month_line_quality[8]
                station_db[code]['minimos_mensuales_quality'][date(year, 10, calendar.monthrange(year, 10)[1])] = month_line_quality[9]
                station_db[code]['minimos_mensuales_quality'][date(year, 11, calendar.monthrange(year, 11)[1])] = month_line_quality[10]
                station_db[code]['minimos_mensuales_quality'][date(year, 12, calendar.monthrange(year, 12)[1])] = month_line_quality[11]

        # check if line is header (new year of data)
        if ' '.join(line.split()) == first_line:
            i = 0

        i += 1   # count

    f.close()
    print(files + ' successfully imported!')

output_folder_path = '/SHDD/Digital Library/05-Hydrology/03-Timeseries/02-Streamflow/01-IDEAM/02-Preprocessed/'

for i in station_db.keys():
    daily_df = pd.DataFrame()
    max_df = pd.DataFrame()
    min_df = pd.DataFrame()

    daily_df['Daily_Streamflow[m3/s]'] = station_db[i]['caudales_diarios']
    daily_df['Data_Quality'] = station_db[i]['caudales_diarios_quality']

    max_df['Max Instantaneous_Streamflow[m3/s]'] = station_db[i]['maximos_mensuales']
    max_df['Data_Quality'] = station_db[i]['maximos_mensuales_quality']

    min_df['Min_Streamflow[m3/s]'] = station_db[i]['minimos_mensuales']
    min_df['Data_Quality'] = station_db[i]['minimos_mensuales_quality']

    daily_df.to_csv(output_folder_path + 'Medios Diarios/' + str(i) + '.csv')
    max_df.to_csv(output_folder_path + 'Maximos Instantaneos/' + str(i) + '.csv')
    min_df.to_csv(output_folder_path + 'Minimos Medios/' + str(i) + '.csv')


# figure out how long the script took to run
endTime = time.time()

print('Execution time: ' + str(round(endTime - startTime, 1)) + ' seconds')
print('*********************************************************************\n')
