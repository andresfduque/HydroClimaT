#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Dec  2 14:48:37 2016

CREATE DATABASE WITH SPECIFIED STRUCTURE
    + Help do display database structure (under construction)

REQUIREMENTS:
    + PostgreSQL 9.6
    + psycopg2 [python module]
    + SQL Alchemy [python module]

CREDITS:
    + Xiaonuo Gantan: http://pythoncentral.io/author/xiaonuo-gantan/
    + Horsburgh, J.S., Tarboton, D.G., CUAHSI Community Observations Data
    Model (ODM), 2008.

@author:    Andres Felipe Duque Perez
email:      andresfduque@gmail.com
"""

# %% Main imports
from sqlalchemy import Column, ForeignKey, Integer, String, Boolean, Text, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
import warnings
warnings.warn("the DatabaseDeclarative module is deprecated", DeprecationWarning,
              stacklevel=2)

# %% Observations Data Model (ODM) implementation in sqlalchemy for Colombia
# def odm_declarative():
'''
Observations Data Model (ODM) implementation in sqlalchemy for Colombia.
Declarative Base.
'''

Base = declarative_base()


# table with Observations Data Model (ODM) version
class ODMVersion(Base):
    __tablename__ = 'ODMversion'
    # Notice that each column is also a normal Python instance attribute.
    VersionNumber = Column(String(50), nullable=False, primary_key=True)


# topic category controlled vocabulary (CV) table
class TopicCategoryCV(Base):
    __tablename__ = 'TopicCategoryCV'
    Term = Column(String(255), primary_key=True)
    Definition = Column(Text)


# metadata table
class ISOMetadata(Base):
    __tablename__ = 'ISOMetadata'
    MetadataId = Column(Integer, primary_key=True)
    TopicCategory = Column(String(255), ForeignKey('TopicCategoryCV.Term'),
                           nullable=False, default='Unknown')
    Title = Column(String(255), nullable=False, default='Unknown')
    Abstract = Column(Text, nullable=False, default='Unknown')
    ProfileVersion = Column(String(255), nullable=False, default='Unknown')
    MetadataLink = Column(String(500), default='Unknown')
    TopicCategory_CV = relationship(TopicCategoryCV)


# sources of information table
class Sources(Base):
    __tablename__ = 'Sources'
    SourceId = Column(Integer, primary_key=True)
    Organization = Column(String(255), nullable=False)
    SourceDescription = Column(Text, nullable=False)
    SourceLink = Column(String(500), default=None)
    ContactName = Column(String(500), nullable=False, default='Unknown')
    Phone = Column(String(255), nullable=False, default='Unknown')
    Email = Column(String(255), nullable=False, default='Unknown')
    Address = Column(String(255), nullable=False, default='Unknown')
    City = Column(String(255), nullable=False, default='Unknown')
    State = Column(String(255), nullable=False, default='Unknown')
    ZipCode = Column(String(255), nullable=False, default='Unknown')
    Citation = Column(Text, nullable=False, default='Unknown')
    MetadataId = Column(Integer, ForeignKey('ISOMetadata.MetadataId'),
                        nullable=False, default=1)
    Isometadata = relationship(ISOMetadata)


# %% Monitoring site locations

# spatial reference (GIS reference system)
class SpatialReferences(Base):
    __tablename__ = 'SpatialReferences'
    # Here we define columns for the table SpatialReferences
    # Notice that each column is also a normal Python instance attribute.
    SpatialReferenceId = Column(Integer, primary_key=True)
    SRSId = Column(Integer, unique=True)
    SRSName = Column(String(255), nullable=False)
    IsGeographic = Column(Boolean)
    Notes = Column(Text)


# vertical datum
class VerticalDatumCV(Base):
    __tablename__ = 'VerticalDatumCV'
    Term = Column(String(255), primary_key=True)
    Definition = Column(Text)


# site type (predefined)
class SiteTypeCV(Base):
    __tablename__ = 'SiteTypeCV'
    Term = Column(String(255), primary_key=True)
    Definition = Column(Text)


# site where observations are made (IDEAM or other agency station)
class Sites(Base):
    __tablename__ = 'Sites'
    SiteId = Column(Integer, primary_key=True)
    SiteCode = Column(String(50), nullable=False)
    SiteName = Column(String(255), nullable=False)
    Latitude = Column(Float, nullable=False)
    Longitude = Column(Float, nullable=False)
    LatLongDatumId = Column(Integer,
                            ForeignKey('SpatialReferences.SpatialReferenceId'),
                            nullable=False)
    VerticalDatum = Column(String(255), ForeignKey('VerticalDatumCV.Term'),
                           default=None)
    Localx = Column(Float, default=None)
    Localy = Column(Float, default=None)
    LocalProjectionId = Column(Integer,
                               ForeignKey('SpatialReferences.SpatialReferenceId'),
                               default=None)
    PosAccuracy_m = Column(Float, default=None)
    State = Column(String(255), default=None)
    County = Column(String(255), default=None)
    Comments = Column(Text, default=None)
    SiteType = Column(String(255), ForeignKey('SiteTypeCV.Term'),
                      default=None)
    LatLongDatum = relationship(SpatialReferences,
                                foreign_keys=[LatLongDatumId])
    LocalProjection = relationship(SpatialReferences,
                                   foreign_keys=[LocalProjectionId])
    VerticalDatum_CV = relationship(VerticalDatumCV)
    SiteType_cv = relationship(SiteTypeCV)


# %% Units

# SI units
class Units(Base):
    __tablename__ = 'Units'
    UnitsId = Column(Integer, primary_key=True)
    UnitsName = Column(String(255), nullable=False)
    UnitsType = Column(String(255), nullable=False)
    UnitsAbbreviation = Column(String(255), nullable=False)


# offset from the site (only when needed)
class OffsetTypes(Base):
    __tablename__ = 'OffsetTypes'
    OffsetTypeId = Column(Integer, primary_key=True)
    OffsetUnits = Column(Integer, ForeignKey('Units.UnitsId'), nullable=False)
    OffsetDescription = Column(Text, nullable=False)
    UnitsRelationship = relationship(Units)


# %% Observed Variables

# variable name
class VariableNameCV(Base):
    __tablename__ = 'VariableNameCV'
    Term = Column(String(255), primary_key=True)
    Definition = Column(Text)


class SpeciationCV(Base):
    __tablename__ = 'SpeciationCV'
    Term = Column(String(255), primary_key=True)
    Definition = Column(Text)


class SampleMediumCV(Base):
    __tablename__ = 'SampleMediumCV'
    Term = Column(String(255), primary_key=True)
    Definition = Column(Text)


class ValueTypeCV(Base):
    __tablename__ = 'ValueTypeCV'
    Term = Column(String(255), primary_key=True)
    Definition = Column(Text)


class DataTypeCV(Base):
    __tablename__ = 'DataTypeCV'
    Term = Column(String(255), primary_key=True)
    Definition = Column(Text)


class GeneralCategoryCV(Base):
    __tablename__ = 'GeneralCategoryCV'
    Term = Column(String(255), primary_key=True)
    Definition = Column(Text)


class Variables(Base):
    __tablename__ = 'Variables'
    VariableId = Column(Integer, primary_key=True)
    VariableCode = Column(String(50), nullable=False)
    VariableName = Column(String(255), ForeignKey('VariableNameCV.Term'),
                          nullable=False)
    Speciation = Column(String(255), ForeignKey('SpeciationCV.Term'),
                        nullable=False, default='Not Applicable')
    VariableUnitsId = Column(Integer, ForeignKey('Units.UnitsId'),
                             nullable=False)
    SampleMedium = Column(String(255), ForeignKey('SampleMediumCV.Term'),
                          nullable=False, default='Unknown')
    ValueType = Column(String(255), ForeignKey('ValueTypeCV.Term'),
                       nullable=False, default='Unknown')
    IsRegular = Column(Boolean, nullable=False, default=False)
    TimeSupport = Column(Float, nullable=False, default=0)
    TimeUnitsId = Column(Integer, ForeignKey('Units.UnitsId'),
                         nullable=False, default=103)
    DataType = Column(String(255), ForeignKey('DataTypeCV.Term'),
                      nullable=False, default='Unknown')
    GeneralCategory = Column(String(255), ForeignKey('GeneralCategoryCV.Term'),
                             nullable=False, default='Unknown')
    NoDataValue = Column(Float, nullable=False, default=-9999)
    VariableName_CV = relationship(VariableNameCV)
    Speciation_CV = relationship(SpeciationCV)
    VariableUnits = relationship(Units, foreign_keys=[VariableUnitsId])
    TimeUnits = relationship(Units, foreign_keys=[TimeUnitsId])
    SampleMedium_CV = relationship(SampleMediumCV)
    ValueType_CV = relationship(ValueTypeCV)
    DataType_CV = relationship(DataTypeCV)
    GeneralCategory_CV = relationship(GeneralCategoryCV)


# %% Categorical data [if used add field to data value table]
class Categories(Base):
    __tablename__ = 'Categories'
    VariableId = Column(Integer, ForeignKey('Variables.VariableId'),
                        primary_key=True)
    DataValue = Column(Float, nullable=False, primary_key=True)
    CategoryDescription = Column(Text, nullable=False, primary_key=True)
    Definition = Column(Text)
    Variable = relationship(Variables)


# %% Data qualifiers
class QualityControlLevels(Base):
    __tablename__ = 'QualityControlLevels'
    QualityControlLevelId = Column(Integer, nullable=False, primary_key=True)
    QualityControlLevelCode = Column(String(50), nullable=False)
    Definition = Column(String(255), nullable=False)
    Explanation = Column(Text, nullable=False)


class Qualifiers(Base):
    __tablename__ = 'Qualifiers'
    QualifierId = Column(Integer, primary_key=True)
    QualifierCode = Column(String(50), default=None)
    QualifierDescription = Column(Text, nullable=False)


# %% Value Grouping [if used add field to data value table]
class GroupsDescription(Base):
    __tablename__ = 'GroupsDescription'
    GroupId = Column(Integer, primary_key=True)
    GroupDescription = Column(Text, nullable=True)


class Groups(Base):
    __tablename__ = 'Groups'
    GroupId = Column(Integer, ForeignKey('GroupsDescription.GroupId'),
                     primary_key=True)
    ValueId = Column(Integer, nullable=True)
    GroupDescription = relationship(GroupsDescription)


# %% Data collection methods
class SampleTypeCV(Base):
    __tablename__ = 'SampleTypeCV'
    Term = Column(String(255), primary_key=True)
    Definition = Column(Text)


class LabMethods(Base):
    __tablename__ = 'LabMethods'
    LabMethodId = Column(Integer, primary_key=True)
    LabName = Column(String(255), nullable=False, default='Unknown')
    LabOrganization = Column(String(255), nullable=False, default='Unknown')
    LabMethodName = Column(String(255), nullable=False, default='Unknown')
    LabMethodDescription = Column(Text, nullable=False, default='Unknown')
    LabMethodLink = Column(String(500), default=None)


class Samples(Base):
    __tablename__ = 'Samples'
    SampleId = Column(Integer, primary_key=True)
    SampleType = Column(String(255), ForeignKey('SampleTypeCV.Term'),
                        nullable=False)
    LabSampleCode = Column(String(50), nullable=False)
    LabMethodId = Column(Integer, ForeignKey('LabMethods.LabMethodId'),
                         nullable=False)
    SampleType_cv = relationship(SampleTypeCV)
    LabMethod = relationship(LabMethods)


class Methods(Base):
    __tablename__ = 'Methods'
    MethodId = Column(Integer, primary_key=True)
    MethodDescription = Column(Text, nullable=False)
    MethodLink = Column(String(500), default=None)


# %% Censor
class CensorCodeCV(Base):
    __tablename__ = 'CensorCodeCV'
    Term = Column(String(255), primary_key=True)
    Definition = Column(Text)


# %% Data values
class DataValues(Base):
    __tablename__ = 'DataValues'
    ValueId = Column(Integer, primary_key=True)
    DataValue = Column(Float, nullable=False)
    ValueAccuracy = Column(Float, default=None)
    LocalDateTime = Column(DateTime, nullable=False)
    UTCOffset = Column(Float, nullable=False)
    DateTimeUTC = Column(DateTime, nullable=False)
    SiteId = Column(Integer, ForeignKey('Sites.SiteId'), nullable=False)
    VariableId = Column(Integer, ForeignKey('Variables.VariableId'),
                        nullable=False)
    OffsetValue = Column(Float, default=None)
    OffsetTypeId = Column(Integer, ForeignKey('OffsetTypes.OffsetTypeId'))
    CensorCode = Column(String(50), ForeignKey('CensorCodeCV.Term'),
                        nullable=False, default='nc')
    QualifierId = Column(Integer, ForeignKey('Qualifiers.QualifierId'),
                         default=None)
    MethodId = Column(Integer, ForeignKey('Methods.MethodId'), nullable=None,
                      default=0)
    SourceId = Column(Integer, ForeignKey('Sources.SourceId'), nullable=False)
    SampleId = Column(Integer, ForeignKey('Samples.SampleId'))
    DerivedFormId = Column(Integer, default=None)
    QualityControlLevelId = Column(Integer,
                                   ForeignKey('QualityControlLevels.QualityControlLevelId'),
                                   nullable=False, default=-9999)
    Variable = relationship(Variables)
    OffsetType = relationship(OffsetTypes)
    CensorCode_CV = relationship(CensorCodeCV)
    Qualifier = relationship(Qualifiers)
    Method = relationship(Methods)
    Source = relationship(Sources)
    QualityControl = relationship(QualityControlLevels)


# %% Derived data
class DerivedForm(Base):
    __tablename__ = 'DerivedForm'
    DerivedFormId = Column(Integer, primary_key=True)
    ValueId = Column(Integer, nullable=False)


# %% Series catalog
class SeriesCatalog(Base):
    __tablename__ = 'SeriesCatalog'
    SeriesId = Column(Integer, primary_key=True)

    # site information
    SiteId = Column(Integer, ForeignKey('Sites.SiteId'))
    SiteCode = Column(String(50))
    SiteName = Column(String(255))
    SiteType = Column(String(255))

    # variable information
    VariableId = Column(Integer, ForeignKey('Variables.VariableId'))
    VariableCode = Column(String(50))
    VariableName = Column(String(255))
    Speciation = Column(String(255))
    VariableUnitsId = Column(Integer, ForeignKey('Units.UnitsId'))
    VariableUnitsName = Column(String(255))
    SampleMedium = Column(String(255))
    ValueType = Column(String(255))
    TimeSupport = Column(Float)
    TimeUnitsId = Column(Integer, ForeignKey('Units.UnitsId'))
    TimeUnitsName = Column(String(255))
    DataType = Column(String(255))
    GeneralCategory = Column(String(255))

    # method information
    MethodId = Column(Integer, ForeignKey('Methods.MethodId'))
    MethodDescription = Column(Text)

    # source information
    SourceId = Column(Integer, ForeignKey('Sources.SourceId'))
    Organization = Column(String(255))
    SourceDescription = Column(Text)
    Citation = Column(Text)

    # quality control levels information
    QualityControlLevelId = Column(Integer, ForeignKey('QualityControlLevels.QualityControlLevelId'))
    QualityControlLevelCode = Column(String(50))

    # other info
    BeginDateTime = Column(DateTime)
    EndDateTime = Column(DateTime)
    BeginDateTimeUTC = Column(DateTime)
    EndDateTimeUTC = Column(DateTime)

    ValueCount = Column(Integer)
