#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jul 13 18:41:25 2017

SERIES EDITOR:
    + Import series
    + Edit series

REQUIREMENTS:
    + PostgreSQL 10.1 or SQLITE3
    + psycopg2 [python module]
    + SQL Alchemy [python module]

@author:    Andrés Felipe Duque Pérez
email:      andresfduque@gmail.com
"""

# %% Main imports

import sys
import numpy as np
from datetime import datetime
from sqlalchemy.orm import sessionmaker
from DatabaseDeclarative import (Base)


# %% Start DBSession
def startDBSession(engine):
    # Bind the engine to the metadata of the Base class so that the
    # declaratives can be accessed through a DBSession instance
    Base.metadata.bind = engine

    DBSession = sessionmaker(bind=engine)
    # A DBSession() instance establishes all conversations with the database
    # and represents a "staging zone" for all the objects loaded into the
    # database session object. Any change made against the objects in the
    # session won't be persisted into the database until you call
    # session.commit(). If you're not happy about the changes, you can
    # revert all of them back to the last commit by calling
    # session.rollback()
    session = DBSession()
    return session


# %% Import IDEAM daily file data
def importIdeamDailyTxt(filepath, engine, methods, variables, source_id, quality_id, censor_term, utc_offset):
    """
        Import data from IDEAM txt file, containing daily data, to POSTGRES database
    """
    # database connection
    global year, code, methodId, utc_date, varIdMean, varIdMax, varIdMin
    conn = engine.connect()

    # base file
    f = open(filepath, 'r')
    first_line = f.readline()
    first_line = ' '.join(first_line.split())
    i = 1

    for line in f:
        # identify each variable in each data block (year, variable)
        if i == 2:
            varIdMean, varIdMax, varIdMin = ideamSupportedVars(line, variables)

        # Get station basic parameters
        if i == 4:
            year = int(line[59:64])  # register year
            code = int(line[104:112])  # station code

        if i == 6:  # get station type
            sta_type = line[48:50]

            # asociate station type in text line with database method
            j = 0
            while j < len(methods['ID']):
                if methods['Description'][j][-2:] == sta_type:
                    methodId = methods['ID'][j]
                j += 1

        # Datavalues for one year are located in these lines
        if 14 <= i <= 44:
            day = int(line[11:13])  # get tha date day form text line
            day_line = split_ideam_line(line)  # get list with 12 datavalues one for each month for that day and year

            # prepare data to be inserted in database
            month = 1  # month counter
            for v in day_line:
                try:  # check if date exist
                    local_date = datetime(year, month, day, 12)
                    utc_date = datetime(year, month, day, np.int(12 + utc_offset))
                except ValueError:
                    local_date = False
                if local_date:
                    if not np.isnan(v[0]):
                        qualifier = 1 if np.isnan(v[1]) else v[1]  # if qualifier is different from 1
                        conn.execute('INSERT INTO "DataValues" ("DataValue", "LocalDateTime", "UTCOffset", '
                                     '"DateTimeUTC", "SiteId", "VariableId", "QualifierId", "MethodId", '
                                     '"SourceId", "QualityControlLevelId", "CensorCode") '
                                     'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                                     (v[0], local_date, utc_offset, utc_date, code, varIdMean, qualifier, methodId,
                                      source_id, quality_id, censor_term))
                month += 1

        if i == 47 and line[0:3] == 'MAX':  # check if "MAXIMO ABSOLUTO" exists (its supposed to be in line 47)
            month_line = split_ideam_line(line)  # max daily instantaneous month flow

            month = 1  # month index
            for v in month_line:
                try:  # check if date exist
                    local_date = datetime(year, month, 1, 12)
                    utc_date = datetime(year, month, 1, np.int(12 + utc_offset))
                except ValueError:
                    local_date = False
                if local_date:
                    if not np.isnan(v[0]):
                        qualifier = 1 if np.isnan(v[1]) else v[1]
                        conn.execute('INSERT INTO "DataValues" ("DataValue", "LocalDateTime", "UTCOffset", '
                                     '"DateTimeUTC", "SiteId", "VariableId", "QualifierId", "MethodId", '
                                     '"SourceId", "QualityControlLevelId", "CensorCode") '
                                     'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                                     (v[0], local_date, utc_offset, utc_date, code, varIdMax, qualifier, methodId,
                                      source_id, quality_id, censor_term))
                month += 1

        elif i == 47 and line[0:3] == 'MIN':  # if "MINIMA MEDIA" exists in the position of the maximum
            month_line = split_ideam_line(line)  # min month flow

            month = 1  # month index
            for v in month_line:
                try:  # check if date exist
                    local_date = datetime(year, month, 1, 12)
                    utc_date = datetime(year, month, 1, np.int(12 + utc_offset))
                except ValueError:
                    local_date = False
                if local_date:
                    if not np.isnan(v[0]):
                        qualifier = 1 if np.isnan(v[1]) else v[1]
                        conn.execute('INSERT INTO "DataValues" ("DataValue", "LocalDateTime", "UTCOffset", '
                                     '"DateTimeUTC", "SiteId", "VariableId", "QualifierId", "MethodId", '
                                     '"SourceId", "QualityControlLevelId", "CensorCode") '
                                     'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                                     (v[0], local_date, utc_offset, utc_date, code, varIdMin, qualifier, methodId,
                                      source_id, quality_id, censor_term))
                month += 1

        if 48 == i and line[0:3] == 'MIN':  # if "MINIMA MEDIA" exists
            month_line = split_ideam_line(line)  # min month flow

            month = 1  # month index
            for v in month_line:
                try:  # check if date exist
                    local_date = datetime(year, month, 1, 12)
                    utc_date = datetime(year, month, 1, np.int(12 + utc_offset))
                except ValueError:
                    local_date = False
                if local_date:
                    if not np.isnan(v[0]):
                        qualifier = 1 if np.isnan(v[1]) else v[1]
                        conn.execute('INSERT INTO "DataValues" ("DataValue", "LocalDateTime", "UTCOffset", '
                                     '"DateTimeUTC", "SiteId", "VariableId", "QualifierId", "MethodId", '
                                     '"SourceId", "QualityControlLevelId", "CensorCode") '
                                     'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                                     (v[0], local_date, utc_offset, utc_date, code, varIdMin, qualifier, methodId,
                                      source_id, quality_id, censor_term))
                month += 1

        # check if line is header (new year of data)
        if ' '.join(line.split()) == first_line:
            i = 0

        i += 1  # count
    #    print('!Archivo ' + files[-13:] + ' Imported!') # convert into a progress bar
    print('completed')
    f.close()


# %% Split IDEAM textfile line
def split_ideam_line(line):
    """
    Split daily discharge data by columns

    line: linea con datos en formato de texto IDEAM
    line_index: índice (python) que indica la posición donde se encuentran \n
    los valores. Debe ser un VECTOR de longitud 12, y cada campo corresponde \n
    a una TUPLE de longitud 2.

    Para los archivos de texto IDEAM la posición de los datos para cada mes es:
        ENE: (18, 26)
        FEB: (27, 35)
        MAR: (36, 44)
        ABR: (45, 53)
        MAY: (54, 62)
        JUN: (63, 71)
        JUL: (72, 80)
        AGO: (81, 89)
        SEP: (90, 98)
        OCT: (99, 107)
        NOV: (108, 116)
        DIC: (117, 125)
    """
    line_index = [(18, 26), (27, 35), (36, 44), (45, 53), (54, 62), (63, 71),
                  (72, 80), (81, 89), (90, 98), (99, 107), (108, 116),
                  (117, 125)]

    # check line index
    if len(line_index) != 12:
        print('!!! El vector de posición no cumple formato [longitud] !!!')
        sys.exit()
    else:
        for i in line_index:
            if len(i) != 2:
                print('!!! El vector de posición no cumple formato [tuple] !!!')
                sys.exit()
            else:
                for j in i:
                    if not str.isnumeric(str(j)):
                        print('!!! El vector de posición no cumple formato \
                        [valores no numericos] !!!')
                        sys.exit()

    # vector de datos
    nan_tuple = (np.nan, np.nan)
    vector = [nan_tuple, nan_tuple, nan_tuple, nan_tuple, nan_tuple, nan_tuple,
              nan_tuple, nan_tuple, nan_tuple, nan_tuple, nan_tuple, nan_tuple]

    # leer los datos de cada mes
    j = 0
    for i in line_index:
        data_qual = np.nan
        if not line[i[0]:i[1]] == '':
            try:
                data = float(line[i[0]:i[1]])
                if str.isnumeric(line[i[1]:i[1] + 1]):
                    data_qual = int(line[i[1]:i[1] + 1])
                else:
                    data_qual = np.nan
            except ValueError:
                data = np.nan
        else:
            data = np.nan
            data_qual = np.nan

        vector[j] = (data, data_qual)
        j += 1

    return vector


# %% Check IDEAM multiple files
def exploreIdeamMultipleFiles(filelist, methods, variables, sites):
    """
        Explore IDEAM listo of txt files:
            + Get number of stations, variables and methods contained
    """
    nStations = 0
    varList = []
    metList = []

    # assume that all variables and methods in each file exist in database
    allSitesCreatedAllFiles = True
    allVarsCreatedAllFiles = True
    allMethodsCreatedAllFiles = True

    # explor each file in list
    for i in filelist:
        fileExplore = exploreIdeamFile(i, methods, variables, sites)
        nSites = fileExplore[0]
        methodsList = fileExplore[1]
        variablesList = fileExplore[2]
        allVarsCreated = fileExplore[3]
        allMethodsCreated = fileExplore[4]
        allSitesCreated = fileExplore[5]
        for j in variablesList:
            if j not in varList:
                varList.append(j)
        for j in methodsList:
            if j not in metList:
                metList.append(j)
        if not allVarsCreated:
            allVarsCreatedAllFiles = False
        if not allMethodsCreated:
            allMethodsCreatedAllFiles = False
        if not allSitesCreated:
            allSitesCreatedAllFiles = False

        nStations += nSites
    nVariables = len(varList)
    nMethods = len(metList)

    return [nStations, nVariables, nMethods, varList, metList, allVarsCreatedAllFiles,
            allMethodsCreatedAllFiles, allSitesCreatedAllFiles]


# %% Check IDEAM file
# noinspection PyShadowingNames
def exploreIdeamFile(filepath, methods, variables, sites):
    """
        Explore IDEAM txt file:
            + Get number of stations, variables and methods contained
    """
    # get first line
    f = open(filepath, 'r')
    first_line = f.readline()
    first_line = ' '.join(first_line.split())
    f.close()

    # open file
    f = open(filepath, 'r')
    i = 0
    codeId = 0
    nSites = 0
    methodsList = []
    variablesList = []

    # assume that all sites, variables and methods exist in database
    allSitesCreated = True
    allVarsCreated = True
    allMethodsCreated = True

    # do for each line
    for line in f:
        # get station basic parameters
        if i == 4:
            code = int(line[104:112])  # station code
            if code != codeId:
                nSites += 1
                codeId = code
                try:
                    sites[code]
                except KeyError:
                    allSitesCreated = False

        # get variables in files
        if i == 2:
            varType = line.split()[1]
            varTimeRes = line.split()[2]
            varName = line.split()[4]

            varName2 = line.split()[0]
            varTimeRes2 = line.split()[1]

            if varType == 'MEDIOS' or varType == 'MEDIA':
                varType = 'Average'
            elif varType == 'MAXIMOS':
                varType = 'Maximum'
            elif varType == 'MINIMOS':
                varType = 'Minimum'
            elif varType == 'TOTALES':
                varType = 'Cumulative'
            else:
                varType = 'Average'

            if varTimeRes == 'DIARIOS' or varTimeRes == 'DIARIA' or varTimeRes2 == 'DIARIO':
                varTimeRes = 'day'
            elif varTimeRes == 'MENSUALES':
                varTimeRes = 'month'

            if varName == 'CAUDALES':
                varName = 'Streamflow'
            elif varName == 'PRECIPITACION':
                varName = 'Precipitation'
            elif varName == 'NIVELES':
                varName = 'Water depth'
            elif varName == 'SEDIMENTOS':
                varName = 'Sediment, suspended'
            elif varName2 == 'TRANSPORTE':
                varName = 'Solids, total suspended'

            # do while
            j = 0
            varExist = 0
            while varExist == 0 and j < len(variables['ID']):
                if variables['Variable'][j] == varName:
                    if variables['Time Resolution'][j] == varTimeRes:
                        if variables['Type'][j] == varType:
                            varExist = 1
                j += 1
            if varExist == 1:
                varId = variables['ID'][j - 1]
                if varId not in variablesList:
                    variablesList.append(varId)
            else:
                allVarsCreated = False

        # get station type
        if i == 6:
            sta_type = line[48:50]

            # do while method dont exist
            j = 0
            methodExist = 0
            while methodExist == 0 and j < len(methods['ID']):
                if methods['Description'][j][-2:] == sta_type:
                    methodExist = 1
                j += 1

            if methodExist == 1:
                methodId = methods['ID'][j - 1]
                if methodId not in methodsList:
                    methodsList.append(methodId)
            else:
                allMethodsCreated = False

        # check if line is header (new year of data)
        if ' '.join(line.split()) == first_line:
            i = 0

        i += 1  # count to check each line

    f.close()
    return [nSites, methodsList, variablesList, allVarsCreated, allMethodsCreated, allSitesCreated]


# noinspection PyShadowingNames
def ideamSupportedVars(ideam_var_line, db_variables):
    varIdMean = None
    varIdMax = None
    varIdMin = None
    varName = ideam_var_line.split()[4]
    varName2 = ideam_var_line.split()[0]

    # supported IDEAM variables
    if varName == 'CAUDALES':
        varName = 'Streamflow'
    elif varName == 'PRECIPITACION':
        varName = 'Precipitation'
    elif varName == 'NIVELES':
        varName = 'Water depth'
    elif varName == 'SEDIMENTOS':
        varName = 'Sediment, suspended'
    elif varName2 == 'TRANSPORTE':
        varName = 'Solids, total suspended'

    # asociate variable in text line with database variable
    j = 0
    while j < len(db_variables['ID']):
        if db_variables['Variable'][j] == varName:
            if db_variables['Type'][j] == 'Average' or db_variables['Type'][j] == 'Cumulative':
                varIdMean = db_variables['ID'][j]
            elif db_variables['Type'][j] == 'Maximum':
                varIdMax = db_variables['ID'][j]
            elif db_variables['Type'][j] == 'Minimum':
                varIdMin = db_variables['ID'][j]
        j += 1
    return varIdMean, varIdMax, varIdMin
