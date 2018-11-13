#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Dec  2 14:48:37 2016

FILL DATABASE TABLES (CV):
    - Fill tables with Controlled Vocabulary (CV)

REQUIREMENTS:
    + PostgreSQL 9.6
    + psycopg2 [python module]
    + SQL Alchemy [python module]

@author:    Andrés Felipe Duque Pérez
email:      andresfduque@gmail.com
"""

# %% Main imports
import os
import numpy as np
import pandas as pd
from sqlalchemy.orm import sessionmaker
from DatabaseDeclarative import (Base, TopicCategoryCV, SpatialReferences, VerticalDatumCV, SiteTypeCV, Units,
                                 VariableNameCV, SpeciationCV, SampleMediumCV, ValueTypeCV, DataTypeCV,
                                 GeneralCategoryCV, SampleTypeCV, CensorCodeCV, ODMVersion)

# %% Tables to be imported
MainPath = os.path.abspath(os.path.join(os.pardir, 'resources/database_structure/Controlled '
                                        'Vocabulary/'))


# %% import controlled vocabulary to database
def importCV(engine=None):
    """"
        Import controlled vocabulary to database
    """
# %% Start DBSession

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


# %% Define database ODM version
    session.add(ODMVersion(VersionNumber='1.0'))
    session.commit()

# %% Insert values for TopicCategoryCV table

    # Import into Pandas DataFrame()
    TopicCategoryPath = MainPath + '/topiccategorycv.csv'
    TopicCategory = pd.read_csv(TopicCategoryPath, index_col=0)

    # Insert values in the topiccategorycv table
    for i in TopicCategory.index:
        session.add(TopicCategoryCV(Term=i,
                                    Definition=TopicCategory['Definition'][i]))
    session.commit()

# %% Insert values for SpatialReferences table

    # Import into Pandas DataFrame()
    SRSPath = MainPath + '/spatialreferences.csv'
    SRS = pd.read_csv(SRSPath, index_col=0)

    # Insert values in the spatialreferences table
    for i in SRS.index:
        session.add(SpatialReferences(SRSId=np.int(SRS['SRSID'][i]),
                                      SRSName=SRS['SRSName'][i],
                                      IsGeographic=bool(SRS['IsGeographic'][i]),
                                      Notes=SRS['Notes'][i]))
    session.commit()

# %% Insert values for VerticalDatumCV table

    # Import into Pandas DataFrame()
    VerticalDatumPath = MainPath + '/verticaldatumcv.csv'
    VDatum = pd.read_csv(VerticalDatumPath, index_col=0)

    # Insert values in the verticaldatumcv table
    for i in VDatum.index:
        session.add(VerticalDatumCV(Term=i,
                                    Definition=VDatum['Definition'][i]))
    session.commit()

# %% Insert values for SiteTypesCV table

    # Import into Pandas DataFrame()
    SiteTypePath = MainPath + '/sitetypecv.csv'
    SiteType = pd.read_csv(SiteTypePath, index_col=0)

    # Insert values in the sitetypecv table
    for i in SiteType.index:
        session.add(SiteTypeCV(Term=i,
                               Definition=SiteType['Definition'][i]))
    session.commit()

# %% Insert values for Units table

    # Import into Pandas DataFrame()
    UnitsPath = MainPath + '/units.csv'
    UnitsDF = pd.read_csv(UnitsPath, index_col=0)

    # Insert values in the units table
    for i in UnitsDF.index:
        session.add(Units(UnitsName=UnitsDF['UnitsName'][i],
                          UnitsType=UnitsDF['UnitsType'][i],
                          UnitsAbbreviation=UnitsDF['UnitsAbbreviation'][i]))
    session.commit()

# %% Insert values for VariableNameCV table

    # Import into Pandas DataFrame()
    VarNamePath = MainPath + '/variablenamecv.csv'
    VarName = pd.read_csv(VarNamePath, index_col=0)

    # Insert values in the variablename table
    for i in VarName.index:
        session.add(VariableNameCV(Term=i,
                                   Definition=VarName['Definition'][i]))
    session.commit()

# %% Insert values for SpeciationCV table

    # Import into Pandas DataFrame()
    SpeciationPath = MainPath + '/speciationcv.csv'
    Speciation = pd.read_csv(SpeciationPath, index_col=0)

    # Insert values in the variablename table
    for i in Speciation.index:
        session.add(SpeciationCV(Term=i,
                                 Definition=Speciation['Definition'][i]))
    session.commit()

# %% Insert values for SampleMediumCV table

    # Import into Pandas DataFrame()
    SampleMediumPath = MainPath + '/samplemediumcv.csv'
    SampleMedium = pd.read_csv(SampleMediumPath, index_col=0)

    # Insert values in the samplemedium table
    for i in SampleMedium.index:
        session.add(SampleMediumCV(Term=i,
                                   Definition=SampleMedium['Definition'][i]))
    session.commit()

# %% Insert values for ValueTypeCV table

    # Import into Pandas DataFrame()
    ValueTypePath = MainPath + '/valuetypecv.csv'
    ValueType = pd.read_csv(ValueTypePath, index_col=0)

    # Insert values in the valuetype table
    for i in ValueType.index:
        session.add(ValueTypeCV(Term=i,
                                Definition=ValueType['Definition'][i]))
    session.commit()

# %% Insert values for DataTypeCV table

    # Import into Pandas DataFrame()
    DataTypePath = MainPath + '/datatypecv.csv'
    DataType = pd.read_csv(DataTypePath, index_col=0)

    # Insert values in the valuetype table
    for i in DataType.index:
        session.add(DataTypeCV(Term=i,
                               Definition=DataType['Definition'][i]))
    session.commit()

# %% Insert values for GeneralCategoryCV table

    # Import into Pandas DataFrame()
    GeneralCategoryPath = MainPath + '/generalcategorycv.csv'
    GeneralCategory = pd.read_csv(GeneralCategoryPath, index_col=0)

    # Insert values in the generalcategory table
    for i in GeneralCategory.index:
        session.add(GeneralCategoryCV(Term=i,
                                      Definition=GeneralCategory['Definition'][i]))
    session.commit()

# %% Insert values for SampleTypeCV table

    # Import into Pandas DataFrame()
    SampleTypePath = MainPath + '/sampletypecv.csv'
    SampleType = pd.read_csv(SampleTypePath, index_col=0)

    # Insert values in the generalcategory table
    for i in SampleType.index:
        session.add(SampleTypeCV(Term=i,
                                 Definition=SampleType['Definition'][i]))
    session.commit()

# %% Insert values for CensorCodeCV table

    # Import into Pandas DataFrame()
    CensorCodePath = MainPath + '/censorcodecv.csv'
    CensorCode = pd.read_csv(CensorCodePath, index_col=0)

    # Insert values in the generalcategory table
    for i in CensorCode.index:
        session.add(CensorCodeCV(Term=i,
                                 Definition=CensorCode['Definition'][i]))
    session.commit()
