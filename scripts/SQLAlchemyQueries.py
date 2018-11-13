#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb 21 21:17:38 2017

PREDEFINED QUERIES:
    - Fill Controlled Vocabulary (CV) tables

REQUIREMENTS:
    + PostgreSQL 9.6
    + psycopg2 [python module]
    + SQL Alchemy [python module]

@author:    Andrés Felipe Duque Pérez
email:      andresfduque@gmail.com
"""

# %% Main imports
# from odm_sqlalchemy_declarative import DataValues
from sqlalchemy import and_
from DatabaseDeclarative import (Base, DataValues, Variables, Units, Sites, Sources, Methods,
                                 QualityControlLevels, Qualifiers, SiteTypeCV, SpatialReferences,
                                 VerticalDatumCV, ISOMetadata, TopicCategoryCV, VariableNameCV,
                                 SpeciationCV, SampleMediumCV, ValueTypeCV, DataTypeCV,
                                 GeneralCategoryCV, CensorCodeCV)
from sqlalchemy.orm import sessionmaker


# %% Start DBSession
def startDBSession(engine):
    # Bind the engine to the metadata of the Base class so that the declarative can be accessed through a DBSession
    # instance
    Base.metadata.bind = engine

    DBSession = sessionmaker(bind=engine)
    # A DBSession() instance establishes all conversations with the database and represents a "staging zone" for all
    # the objects loaded into the database session object. Any change made against the objects in the session won't be
    # persisted into the database until you call session.commit(). If you're not happy about the changes, you can
    # revert all of them back to the last commit by calling session.rollback()
    session = DBSession()
    return session


# %% Create methods dictionary
def get_metadata_table(engine=None):
    """
        Create metadata dictionary to be displayed in Hydro-ClimaT
        [ID, Description]
    """
    metadataDictionary = None

    if engine:
        session = startDBSession(engine)

        # create a dictionary containing data from all sites in the database (not necessary that
        # each site have data-values)
        metadataId = [i.MetadataId for i in session.query(ISOMetadata).all()]
        metadataTitle = [i.Title for i in session.query(ISOMetadata).all()]
        metadataTopic = [i.TopicCategory for i in session.query(ISOMetadata).all()]
        metadataAbstract = [i.Abstract for i in session.query(ISOMetadata).all()]
        metadataProfileVersion = [i.ProfileVersion for i in session.query(ISOMetadata).all()]
        metadataLink = [i.MetadataLink for i in session.query(ISOMetadata).all()]

        metadataDictionary = {'ID': metadataId, 'Topic': metadataTopic, 'Title': metadataTitle,
                              'Abstract': metadataAbstract, 'Link': metadataLink,
                              'ProfilerVersion': metadataProfileVersion}

    return metadataDictionary


# %% Create sources dictionary
def get_sources_table(engine=None):
    """
        Create sources dictionary to be displayed in Hydro-ClimaT
        [ID, Organization , Description]
    """
    sourcesDictionary = None

    if engine:
        session = startDBSession(engine)

        source_id = [i.SourceId for i in session.query(Sources).all()]
        source_name = [i.Organization for i in session.query(Sources).all()]
        source_desc = [i.SourceDescription for i in session.query(Sources).all()]
        source_meta = [i.MetadataId for i in session.query(Sources).all()]

        sourcesDictionary = {'ID': source_id, 'Organization': source_name,
                             'Description': source_desc, 'Metadata ID': source_meta}

    return sourcesDictionary


# %% Create sites dictionary
def get_sites_table(engine=None):
    """
        Create sites dictionary to be displayed in Hydro-ClimaT
        sitesDict[siteId] = [Code, Name, Lat, Lon]
    """
    sitesDict = None

    if engine:
        session = startDBSession(engine)

        sitesDict = {}
        for i in session.query(Sites).all():
            sitesDict[i.SiteId] = ([i.SiteId, i.SiteName, i.Longitude, i.Latitude])

    return sitesDict


# %% Create variables dictionary
def get_vars_table(engine=None):
    """
        Create variables dictionary to be displayed in Hydro-ClimaT
        [ID, Number, Name, Units, Time Resolution, Data Type, No Data Value]
    """
    varsDictionary = None

    if engine:
        session = startDBSession(engine)

        # create a dictionary containing data from all sites in the database (not necessary that
        # each site have data-values)
        varId = [i.VariableId for i in session.query(Variables).all()]
        varName = [i.VariableName for i in session.query(Variables).all()]
        varType = [i.DataType for i in session.query(Variables).all()]
        varNdata = [i.NoDataValue for i in session.query(Variables).all()]
        varUnitsId = [i.VariableUnitsId for i in session.query(Variables).all()]
        varTunitsId = [i.TimeUnitsId for i in session.query(Variables).all()]
        varUnits = [session.query(Units).filter(Units.UnitsId == i).one().UnitsName
                    for i in varUnitsId]
        varTunits = [session.query(Units).filter(Units.UnitsId == i).one().UnitsName
                     for i in varTunitsId]

        varsDictionary = {'ID': varId, 'Variable': varName, 'Units': varUnits,
                          'Time Resolution': varTunits, 'Type': varType, 'No Data': varNdata}
    return varsDictionary


# %% Create methods dictionary
def get_methods_table(engine=None):
    """
        Create methods dictionary to be displayed in Hydro-ClimaT
        [ID, Description]
    """
    methodsDictionary = None

    if engine:
        session = startDBSession(engine)

        # create a dictionary containing data from all sites in the database (not necessary that
        # each site have data-values)
        methodId = [i.MethodId for i in session.query(Methods).all()]
        methodDescription = [i.MethodDescription for i in session.query(Methods).all()]
        methodLink = [i.MethodLink for i in session.query(Methods).all()]

        methodsDictionary = {'ID': methodId, 'Description': methodDescription, 'Link': methodLink}

    return methodsDictionary


# %% Create quality control levels dictionary
def get_qualities_table(engine=None):
    """
        Create quality control levels dictionary to be displayed in Hydro-ClimaT
        [ID, Definition, Explanation]
    """
    qualityDictionary = None

    if engine:
        session = startDBSession(engine)

        # create a dictionary containing data from all sites in the database (not necessary that
        # each site have data-values)
        qualityId = [i.QualityControlLevelId for i in session.query(QualityControlLevels).all()]
        qualityCode = [i.QualityControlLevelCode for i in session.query(QualityControlLevels).all()]
        qualityDefinition = [i.Definition for i in session.query(QualityControlLevels).all()]
        qualityExplanation = [i.Explanation for i in session.query(QualityControlLevels).all()]

        qualityDictionary = {'ID': qualityId, 'Code': qualityCode, 'Definition': qualityDefinition,
                             'Explanation': qualityExplanation}

    return qualityDictionary


# %% Create qualifiers dictionary
def get_qualifiers_table(engine=None):
    """
        Create qualifiers dictionary to be displayed in Hydro-ClimaT
        [ID, Description]
    """
    qualifiersDictionary = None

    if engine:
        session = startDBSession(engine)

        # create a dictionary containing data from all sites in the database (not necessary that
        # each site have data-values)
        qualifierId = [i.QualifierId for i in session.query(Qualifiers).all()]
        qualifierCode = [i.QualifierCode for i in session.query(Qualifiers).all()]
        qualifierDescription = [i.QualifierDescription for i in session.query(Qualifiers).all()]

        qualifiersDictionary = {'ID': qualifierId, 'Code': qualifierCode,
                                'Description': qualifierDescription}

    return qualifiersDictionary


# %% Create site-types dictionary
def get_sitetype_table(engine=None):
    """
        Create site-types dictionary to be displayed in Hydro-ClimaT
        [Term]
    """
    sitetypesDictionary = None

    if engine:
        session = startDBSession(engine)

        # create a dictionary containing data from all sites in the database (not necessary that
        # each site have data-values)
        siteTypes = [i.Term for i in session.query(SiteTypeCV).all()]

        sitetypesDictionary = {'Term': siteTypes}

    return sitetypesDictionary


# %% Create spatial references dictionary
def get_srs_table(engine=None):
    """
        Create spatial references dictionary to be displayed in Hydro-ClimaT
        [ID, SRSName]
    """
    srsDictionary = None

    if engine:
        session = startDBSession(engine)

        # create a dictionary containing data from all sites in the database (not necessary that
        # each site have data-values)
        srsId = [i.SRSId for i in session.query(SpatialReferences).all()]
        srsName = [i.SRSName for i in session.query(SpatialReferences).all()]
        SpatialReferenceId = [i.SpatialReferenceId for i in session.query(SpatialReferences).all()]

        srsDictionary = {'FID': SpatialReferenceId, 'ID': srsId, 'SRSName': srsName}

    return srsDictionary


# %% Create sitetypes dictionary
def get_vdatum_table(engine=None):
    """
        Create vertical datum dictionary to be displayed in Hydro-ClimaT
        [Term]
    """
    vdatumDictionary = None

    if engine:
        session = startDBSession(engine)

        # create a dictionary containing data from all sites in the database (not necessary that
        # each site have data-values)
        vdatum = [i.Term for i in session.query(VerticalDatumCV).all()]

        vdatumDictionary = {'Term': vdatum}

    return vdatumDictionary


# %% Create topic category dictionary
def get_topicCategory_table(engine=None):
    """
        Create topic category dictionary to be displayed in Hydro-ClimaT
        [Term]
    """
    topicCategory = None

    if engine:
        session = startDBSession(engine)
        topicCategory = [i.Term for i in session.query(TopicCategoryCV).all()]

    return {'Term': topicCategory}


# %% Create variable name dictionary
def get_varName_table(engine=None):
    """
        Create variable name dictionary to be displayed in Hydro-ClimaT
        [Term]
    """
    varName = None

    if engine:
        session = startDBSession(engine)
        varName = [i.Term for i in session.query(VariableNameCV).all()]

    return {'Term': varName}


# %% Create speciation name dictionary
def get_speciation_table(engine=None):
    """
        Create variable name dictionary to be displayed in Hydro-ClimaT
        [Term]
    """
    speciation = None

    if engine:
        session = startDBSession(engine)
        speciation = [i.Term for i in session.query(SpeciationCV).all()]

    return {'Term': speciation}


# %% Create units name dictionary
def get_units_table(engine=None):
    """
        Create units name dictionary to be displayed in Hydro-ClimaT
        [ID, Name]
    """
    units = None
    uName = None

    if engine:
        session = startDBSession(engine)
        units = [i.UnitsId for i in session.query(Units).all()]
        uName = [i.UnitsName for i in session.query(Units).all()]

    return {'ID': units, 'Name': uName}


# %% Create sample medium dictionary
def get_sampleMedium_table(engine=None):
    """
        Create sample medium dictionary to be displayed in Hydro-ClimaT
        [Term]
    """
    sampleMedium = None

    if engine:
        session = startDBSession(engine)
        sampleMedium = [i.Term for i in session.query(SampleMediumCV).all()]

    return {'Term': sampleMedium}


# %% Create value type dictionary
def get_valueType_table(engine=None):
    """
        Create value type dictionary to be displayed in Hydro-ClimaT
        [Term]
    """
    valueType = None

    if engine:
        session = startDBSession(engine)
        valueType = [i.Term for i in session.query(ValueTypeCV).all()]

    return {'Term': valueType}


# %% Create data type dictionary
def get_dataType_table(engine=None):
    """
        Create data type dictionary to be displayed in Hydro-ClimaT
        [Term]
    """
    dataType = None

    if engine:
        session = startDBSession(engine)
        dataType = [i.Term for i in session.query(DataTypeCV).all()]

    return {'Term': dataType}


# %% Create category type dictionary
def get_category_table(engine=None):
    """
        Create general category dictionary to be displayed in Hydro-ClimaT
        [Term]
    """
    category = None

    if engine:
        session = startDBSession(engine)
        category = [i.Term for i in session.query(GeneralCategoryCV).all()]

    return {'Term': category}


# %% Create censor code dictionary
def get_censor_table(engine=None):
    """
        Create censor code dictionary to be displayed in Hydro-ClimaT
        [Term]
    """
    censorTerm = None
    censorDef = None

    if engine:
        session = startDBSession(engine)
        censorTerm = [i.Term for i in session.query(CensorCodeCV).all()]
        censorDef = [i.Definition for i in session.query(CensorCodeCV).all()]

    return {'Term': censorTerm, 'Definition': censorDef}


# %% get sites series information
def sitesQuery(site_id, engine=None):
    """
        Create sites information query to display in HydroClimaT
        [Sources, Variables, Quality control levels, Qualifiers]
    """
    sourcesId = None
    methodsId = None
    variablesId = None
    methodsName = None
    qualitiesId = None
    sourcesName = None
    variablesName = None
    qualitiesName = None
    qualitiesDescription = None

    import numpy as np

    if engine:
        session = startDBSession(engine)

        # query all data-values related data id from a specific site [sources, variables, methods, quality]
        tempTable = session.query(DataValues).filter(DataValues.SiteId == int(site_id)).all()
        sourcesId = np.unique([i.SourceId for i in tempTable])
        variablesId = np.unique([i.VariableId for i in tempTable])
        methodsId = np.unique([i.MethodId for i in tempTable])
        qualitiesId = np.unique([i.QualityControlLevelId for i in tempTable])

        # query related data names with specific id's
        sourcesName = []
        variablesName = []
        methodsName = []
        qualitiesName = []
        qualitiesDescription = []

        for i in sourcesId:
            sourcesName.append(session.query(Sources).filter(Sources.SourceId ==
                               int(i)).one().Organization)

        for i in variablesId:
            varname = session.query(Variables).filter(Variables.VariableId ==
                                                      int(i)).one().VariableName
            vartype = session.query(Variables).filter(Variables.VariableId ==
                                                      int(i)).one().DataType
            vartunitsid = session.query(Variables).filter(Variables.VariableId ==
                                                          int(i)).one().TimeUnitsId
            vartunits = session.query(Units).filter(Units.UnitsId ==
                                                    int(vartunitsid)).one().UnitsName
            variablesName.append(varname + ' - ' + vartype + ' (' + vartunits + ')')

        for i in methodsId:
            methodsName.append(session.query(Methods).filter(Methods.MethodId ==
                               int(i)).one().MethodDescription)

        for i in qualitiesId:
            qualitiesName.append(session.query(QualityControlLevels).filter(
                    QualityControlLevels.QualityControlLevelId == int(i)).one().Definition)
            qualitiesDescription.append(session.query(QualityControlLevels).filter(
                    QualityControlLevels.QualityControlLevelId == int(i)).one().Explanation)

    return {'sourcesId': sourcesId, 'variablesId': variablesId, 'methodsId': methodsId,
            'qualitiesId': qualitiesId, 'sourcesName': sourcesName, 'variablesName': variablesName,
            'methodsName': methodsName, 'qualitiesName': qualitiesName,
            'qualitiesDescription': qualitiesDescription}


# %% get time-series series
# noinspection PyUnresolvedReferences
def timeSeriesQuery(search_parameters, engine=None):
    """
        Get time-series from database
        [Date, Data-value]
    """
    pd_timeseries = None

    import pandas as pd
    import numpy as np
    import calendar

    if engine:
        session = startDBSession(engine)

        # search parameters
        sourceId = int(search_parameters[0][1:3])
        variableId = int(search_parameters[1][1:3])
        methodId = int(search_parameters[2][1:3])
        qualityId = int(search_parameters[3][1:3])
        siteId = int(search_parameters[4][0])

        # relate variable, method, source and quality (text) to the identifiers
        query = session.query(DataValues).filter(and_(
                DataValues.SiteId == siteId, DataValues.VariableId == variableId,
                DataValues.MethodId == methodId, DataValues.QualityControlLevelId == qualityId,
                DataValues.SourceId == sourceId)).all()
        data = [i.DataValue for i in query]
        dates = [i.LocalDateTime for i in query]

        # check temporal resolution of the timeseries
        tempRes = session.query(Variables).filter(Variables.VariableId == variableId).one()
        tempRes = tempRes.TimeUnitsId

        # create pandas timeseries to flush data
        start_date = np.min(dates)
        end_date = np.max(dates)

        start_date = pd.datetime(start_date.year, 1, 1)
        end_date = pd.datetime(end_date.year, 12, 31)

        if tempRes == 104:  # daily data
            daterange = pd.date_range(start_date, end_date)     # date index
            nan_vector = np.zeros([len(daterange)])               # empty vector
            nan_vector[:] = np.nan                                # empty vector
            pd_timeseries = pd.Series(nan_vector, daterange)      # empty timeseries

            # flush data-values in pandas timeseries by date index
            for m in range(0, len(data)):
                datetime = pd.datetime(dates[m].year, dates[m].month, dates[m].day)
                pd_timeseries[datetime] = data[m]

        elif tempRes == 106:   # monthly data
            daterange = pd.date_range(start_date, end_date, freq='M')   # date index
            nan_vector = np.zeros([len(daterange)])                       # empty vector
            nan_vector[:] = np.nan                                        # empty vector
            pd_timeseries = pd.Series(nan_vector, daterange)              # empty timeseries

            # flush datavalues in pandas timeseries by date index
            for m in range(0, len(data)):
                last_day = calendar.monthrange(dates[m].year, dates[m].month)[1]
                datetime = pd.datetime(dates[m].year, dates[m].month, last_day)
                pd_timeseries[datetime] = data[m]

    return pd_timeseries
