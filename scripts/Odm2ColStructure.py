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
# Add people, organization and affiliations
# ======================================================================================================================
people_csv = pd.read_csv(odm2_structure + 'People/people.csv')
objects_people = []
for i in people_csv.index.values:
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

organizations_csv = pd.read_csv(odm2_structure + 'Organizations/organizations.csv')
objects_organizations = []
for i in organizations_csv.index.values:
    try:
        obj_organization = None
        obj_organization = models.Organizations()
        obj_organization.OrganizationID = str(organizations_csv['OrganizationID'][i])
        obj_organization.OrganizationTypeCV = organizations_csv['OrganizationTypeCV'][i]
        obj_organization.OrganizationCode = str(organizations_csv['OrganizationCode'][i])
        obj_organization.OrganizationName = organizations_csv['OrganizationName'][i]
        if not pd.isna(organizations_csv['OrganizationDescription'][i]):
            obj_organization.OrganizationDescription = organizations_csv['OrganizationDescription'][i]
        if not pd.isna(organizations_csv['OrganizationLink'][i]):
            obj_organization.OrganizationLink = organizations_csv['OrganizationLink'][i]
        if not pd.isna(organizations_csv['ParentOrganizationID'][i]):
            obj_organization.ParentOrganizationID = organizations_csv['ParentOrganizationID'][i]
        objects_organizations.append(obj_organization)
    except Exception as e:
        session.rollback()
        if obj_organization.OrganizationID is not None:
            print("issue loading single object %s: %s " % (obj_organization.OrganizationID, e))
        pass
session.add_all(objects_organizations)
if not args.debug:
    session.commit()

affiliations_csv = pd.read_csv(odm2_structure + 'Affiliations/affiliations.csv')
objects_affiliations = []
for i in affiliations_csv.index.values:
    try:
        obj_affiliation = None
        obj_affiliation = models.Affiliations()
        obj_affiliation.AffiliationID = str(affiliations_csv['AffiliationID'][i])
        obj_affiliation.PersonID = str(affiliations_csv['PersonID'][i])
        obj_affiliation.AffiliationStartDate = str(affiliations_csv['AffiliationStartDate'][i])
        obj_affiliation.PrimaryEmail = affiliations_csv['PrimaryEmail'][i]
        if not pd.isna(affiliations_csv['OrganizationID'][i]):
            obj_affiliation.OrganizationID = str(affiliations_csv['OrganizationID'][i])
        if not pd.isna(affiliations_csv['AffiliationEndDate'][i]):
            obj_affiliation.AffiliationEndDate = str(affiliations_csv['AffiliationEndDate'][i])
        if not pd.isna(affiliations_csv['IsPrimaryOrganizationContact'][i]):
            obj_affiliation.IsPrimaryOrganizationContact = affiliations_csv['IsPrimaryOrganizationContact'][i]
        if not pd.isna(affiliations_csv['PrimaryPhone'][i]):
            obj_affiliation.PrimaryPhone = str(affiliations_csv['PrimaryPhone'][i])
        if not pd.isna(affiliations_csv['PrimaryAddress'][i]):
            obj_affiliation.PrimaryAddress = str(affiliations_csv['PrimaryAddress'][i])
        if not pd.isna(affiliations_csv['PersonLink'][i]):
            obj_affiliation.PersonLink = str(affiliations_csv['PersonLink'][i])
        objects_affiliations.append(obj_affiliation)
    except Exception as e:
        session.rollback()
        if obj_affiliation.AffiliationID is not None:
            print("issue loading single object %s: %s " % (obj_affiliation.AffiliationID, e))
        pass
session.add_all(objects_affiliations)
if not args.debug:
    session.commit()

# ======================================================================================================================
# Spatial references
# ======================================================================================================================
srs_csv = pd.read_csv(odm2_structure + 'SpatialReferences/spatialreferences.csv')
objects_srs = []
for i in srs_csv.index.values:
    try:
        obj_srs = None
        obj_srs = models.SpatialReferences()
        obj_srs.SRSName = srs_csv['SRSName'][i]
        obj_srs.SpatialReferenceID = str(srs_csv['SpatialReferenceID'][i])
        if not pd.isna(srs_csv['SRSCode'][i]):
            obj_srs.SRSCode = str(srs_csv['SRSCode'][i])
        if not pd.isna(srs_csv['SRSDescription'][i]):
            obj_srs.SRSDescription = srs_csv['SRSDescription'][i]
        if not pd.isna(srs_csv['SRSLink'][i]):
            obj_srs.SRSLink = srs_csv['SRSLink'][i]
        objects_srs.append(obj_srs)
    except Exception as e:
        session.rollback()
        if obj_srs.SpatialReferenceID is not None:
            print("issue loading single object %s: %s " % (obj_srs.SpatialReferenceID, e))
        pass
session.add_all(objects_srs)
if not args.debug:
    session.commit()

elevation_datum_csv = pd.read_csv(odm2_structure + 'SpatialReferences/cvelevationdatum.csv')
objects_elevation_datum = []
for i in elevation_datum_csv.index.values:
    try:
        obj_elevation_datum = None
        obj_elevation_datum = models.CVElevationDatum()
        obj_elevation_datum.Name = elevation_datum_csv['Name'][i]
        obj_elevation_datum.Term = elevation_datum_csv['Term'][i]
        if not pd.isna(elevation_datum_csv['Definition'][i]):
            obj_elevation_datum.Definition = elevation_datum_csv['Definition'][i]
        if not pd.isna(elevation_datum_csv['Category'][i]):
            obj_elevation_datum.Category = elevation_datum_csv['Category'][i]
        if not pd.isna(elevation_datum_csv['SourceVocabularyURI'][i]):
            obj_elevation_datum.SourceVocabularyURI = elevation_datum_csv['SourceVocabularyURI'][i]
        objects_elevation_datum.append(obj_elevation_datum)
    except Exception as e:
        session.rollback()
        if obj_elevation_datum.Name is not None:
            print("issue loading single object %s: %s " % (obj_elevation_datum.Name, e))
        pass
session.add_all(objects_elevation_datum)
if not args.debug:
    session.commit()

# ======================================================================================================================
# Units and Variables
# ======================================================================================================================
units_csv = pd.read_csv(odm2_structure + 'Units/units.csv')
objects_units = []
for i in units_csv.index.values:
    try:
        obj_units = None
        obj_units = models.Units()
        obj_units.UnitsID = str(units_csv['UnitsID'][i])
        obj_units.UnitsTypeCV = units_csv['UnitsTypeCV'][i]
        obj_units.UnitsAbbreviation = units_csv['UnitsAbbreviation'][i]
        obj_units.UnitsName = units_csv['UnitsName'][i]
        if not pd.isna(units_csv['UnitsLink'][i]):
            obj_units.UnitsLink = units_csv['UnitsLink'][i]
        objects_units.append(obj_units)
    except Exception as e:
        session.rollback()
        if obj_units.UnitsID is not None:
            print("issue loading single object %s: %s " % (obj_units.UnitsID, e))
        pass
session.add_all(objects_units)
if not args.debug:
    session.commit()

cv_variables_csv = pd.read_csv(odm2_structure + 'Variables/cv_variablename.csv')
objects_cv_variables = []
for i in cv_variables_csv.index.values:
    try:
        obj_variable = None
        obj_variable = models.CVVariableName()
        obj_variable.Name = cv_variables_csv['Name'][i]
        obj_variable.Term = cv_variables_csv['Term'][i]
        if not pd.isna(cv_variables_csv['Definition'][i]):
            obj_variable.Definition = cv_variables_csv['Definition'][i]
        if not pd.isna(cv_variables_csv['Category'][i]):
            obj_variable.Category = cv_variables_csv['Category'][i]
        if not pd.isna(cv_variables_csv['SourceVocabularyURI'][i]):
            obj_variable.SourceVocabularyURI = cv_variables_csv['SourceVocabularyURI'][i]
        objects_cv_variables.append(obj_variable)
    except Exception as e:
        session.rollback()
        if obj_variable.Name is not None:
            print("issue loading single object %s: %s " % (obj_variable.Name, e))
        pass
session.add_all(objects_cv_variables)
if not args.debug:
    session.commit()

variable_csv = pd.read_csv(odm2_structure + 'Variables/variables.csv')
objects_variable = []
for i in variable_csv.index.values:
    try:
        obj_variable = None
        obj_variable = models.Variables()
        obj_variable.VariableID = str(variable_csv['VariableID'][i])
        obj_variable.VariableTypeCV = variable_csv['VariableTypeCV'][i]
        obj_variable.VariableCode = str(variable_csv['VariableCode'][i])
        obj_variable.VariableNameCV = variable_csv['VariableNameCV'][i]
        obj_variable.NoDataValue = str(variable_csv['NoDataValue'][i])
        if not pd.isna(variable_csv['VariableDefinition'][i]):
            obj_variable.VariableDefinition = variable_csv['VariableDefinition'][i]
        if not pd.isna(variable_csv['SpeciationCV'][i]):
            obj_variable.SpeciationCV = variable_csv['SpeciationCV'][i]
        objects_variable.append(obj_variable)
    except Exception as e:
        session.rollback()
        if obj_variable.VariableID is not None:
            print("issue loading single object %s: %s " % (obj_variable.VariableID, e))
        pass
session.add_all(objects_variable)
if not args.debug:
    session.commit()

# ======================================================================================================================
# Methods
# ======================================================================================================================
method_csv = pd.read_csv(odm2_structure + 'Methods/methods.csv')
objects_method = []
for i in method_csv.index.values:
    try:
        obj_method = None
        obj_method = models.Methods()
        obj_method.MethodID = str(method_csv['MethodID'][i])
        obj_method.MethodTypeCV = method_csv['MethodTypeCV'][i]
        obj_method.MethodCode = method_csv['MethodCode'][i]
        obj_method.MethodName = method_csv['MethodName'][i]
        obj_method.OrganizationID = str(method_csv['OrganizationID'][i])
        if not pd.isna(method_csv['MethodDescription'][i]):
            obj_method.MethodDescription = method_csv['MethodDescription'][i]
        if not pd.isna(method_csv['MethodLink'][i]):
            obj_method.MethodLink = method_csv['MethodLink'][i]
        objects_method.append(obj_method)
    except Exception as e:
        session.rollback()
        if obj_method.MethodID is not None:
            print("issue loading single object %s: %s " % (obj_method.MethodID, e))
        pass
session.add_all(objects_method)
if not args.debug:
    session.commit()

# ======================================================================================================================
# Sampling Features and Sites
# ======================================================================================================================
feature_actions_csv = pd.read_csv(odm2_structure + 'FeatureActions/featureactionsIDEAM.csv')
objects_sites = []
for i in feature_actions_csv.index.values:
    try:
        obj_site = None
        obj_site = models.Sites()
        obj_site.SamplingFeatureID = str(feature_actions_csv['SamplingFeatureID'][i])
        obj_site.SamplingFeatureUUID = uuid.uuid4()
        obj_site.SamplingFeatureTypeCV = feature_actions_csv['SamplingFeatureTypeCV'][i]
        obj_site.SamplingFeatureCode = str(feature_actions_csv['SamplingFeatureCode'][i])
        obj_site.SamplingFeatureName = str(feature_actions_csv['SamplingFeatureName'][i])
        obj_site.SamplingFeatureDescription = feature_actions_csv['SamplingFeatureDescription'][i]
        obj_site.SamplingFeatureGeotypeCV = feature_actions_csv['SamplingFeatureGeotypeCV'][i]
        if not pd.isna(feature_actions_csv['Elevation_m'][i]):
            obj_site.Elevation_m = str(feature_actions_csv['Elevation_m'][i])
        if not pd.isna(feature_actions_csv['ElevationDatumCV'][i]):
            obj_site.ElevationDatumCV = feature_actions_csv['ElevationDatumCV'][i]
        if not pd.isna(feature_actions_csv['FeatureGeometryWKT'][i]):
            obj_site.FeatureGeometryWKT = str(feature_actions_csv['FeatureGeometryWKT'][i])
            obj_site.FeatureGeometry = str(feature_actions_csv['FeatureGeometryWKT'][i])

        obj_site.SpatialReferenceID = str(feature_actions_csv['SpatialReferenceID'][i])
        obj_site.SiteTypeCV = feature_actions_csv['SiteTypeCV'][i]
        obj_site.Latitude = str(feature_actions_csv['Latitude'][i])
        obj_site.Longitude = str(feature_actions_csv['Longitude'][i])
        objects_sites.append(obj_site)
    except Exception as e:
        session.rollback()
        if obj_site.SamplingFeatureID is not None:
            print("issue loading single object %s: %s " % (obj_site.SamplingFeatureID, e))
        pass
session.add_all(objects_sites)
if not args.debug:
    session.commit()

# ======================================================================================================================
# Actions
# ======================================================================================================================
actions_csv = pd.read_csv(odm2_structure + 'Actions/actions.csv')
objects_actions = []
for i in actions_csv.index.values:
    try:
        obj_action = None
        obj_action = models.Actions()
        obj_action.ActionID = str(actions_csv['ActionID'][i])
        obj_action.ActionTypeCV = actions_csv['ActionTypeCV'][i]
        obj_action.MethodID = str(actions_csv['MethodID'][i])
        obj_action.BeginDateTime = str(actions_csv['BeginDateTime'][i])
        obj_action.BeginDateTimeUTCOffset = str(actions_csv['BeginDateTimeUTCOffset'][i])
        if not pd.isna(actions_csv['EndDateTime'][i]):
            obj_action.EndDateTime = str(actions_csv['EndDateTime'][i])
            obj_action.EndDateTimeUTCOffset = str(actions_csv['EndDateTimeUTCOffset'][i])
        if not pd.isna(actions_csv['ActionDescription'][i]):
            obj_action.ActionDescription = actions_csv['ActionDescription'][i]
        if not pd.isna(actions_csv['ActionFileLink'][i]):
            obj_action.ActionFileLink = actions_csv['ActionFileLink'][i]
        objects_actions.append(obj_action)
    except Exception as e:
        session.rollback()
        if obj_action.ActionID is not None:
            print("issue loading single object %s: %s " % (obj_action.ActionID, e))
        pass
session.add_all(objects_actions)
if not args.debug:
    session.commit()


actions_by_csv = pd.read_csv(odm2_structure + 'ActionBy/actionby.csv')
objects_actions_by = []
for i in actions_by_csv.index.values:
    try:
        obj_actionby = None
        obj_actionby = models.ActionBy()
        obj_actionby.BridgeID = str(actions_by_csv['BridgeID'][i])
        obj_actionby.ActionID = str(actions_by_csv['ActionID'][i])
        obj_actionby.AffiliationID = str(actions_by_csv['AffiliationID'][i])
        obj_actionby.IsActionLead = actions_by_csv['IsActionLead'][i]
        if not pd.isna(actions_by_csv['RoleDescription'][i]):
            obj_actionby.RoleDescription = actions_by_csv['RoleDescription'][i]
        objects_actions_by.append(obj_actionby)
    except Exception as e:
        session.rollback()
        if obj_actionby.BridgeID is not None:
            print("issue loading single object %s: %s " % (obj_actionby.BridgeID, e))
        pass
session.add_all(objects_actions_by)
if not args.debug:
    session.commit()


objects_feature_actions = []
for i in feature_actions_csv.index.values:
    try:
        obj_feature_action = None
        obj_feature_action = models.FeatureActions()
        obj_feature_action.FeatureActionID = str(feature_actions_csv['FeatureActionID'][i])
        obj_feature_action.SamplingFeatureID = str(feature_actions_csv['SamplingFeatureID'][i])
        obj_feature_action.ActionID = str(feature_actions_csv['ActionID'][i])
        objects_feature_actions.append(obj_feature_action)
    except Exception as e:
        session.rollback()
        if obj_feature_action.FeatureActionID is not None:
            print("issue loading single object %s: %s " % (obj_feature_action.FeatureActionID, e))
        pass
session.add_all(objects_feature_actions)
if not args.debug:
    session.commit()

# ======================================================================================================================
# Processing Levels and data quality
# ======================================================================================================================
processing_levels_csv = pd.read_csv(odm2_structure + 'ProcessingLevels/processinglevels.csv')
objects_processing_levels = []
for i in processing_levels_csv.index.values:
    try:
        obj_processing_level = None
        obj_processing_level = models.ProcessingLevels()
        obj_processing_level.ProcessingLevelID = str(processing_levels_csv['ProcessingLevelID'][i])
        obj_processing_level.ProcessingLevelCode = str(processing_levels_csv['ProcessingLevelCode'][i])
        if not pd.isna(processing_levels_csv['Definition'][i]):
            obj_processing_level.Definition = processing_levels_csv['Definition'][i]
        if not pd.isna(processing_levels_csv['Explanation'][i]):
            obj_processing_level.Explanation = processing_levels_csv['Explanation'][i]
        objects_processing_levels.append(obj_processing_level)
    except Exception as e:
        session.rollback()
        if obj_processing_level.ActionID is not None:
            print("issue loading single object %s: %s " % (obj_processing_level.ActionID, e))
        pass
session.add_all(objects_processing_levels)
if not args.debug:
    session.commit()

data_qualities_csv = pd.read_csv(odm2_structure + 'DataQuality/dataquality.csv')
objects_data_qualities = []
for i in data_qualities_csv.index.values:
    try:
        obj_data_quality = None
        obj_data_quality = models.DataQuality()
        obj_data_quality.DataQualityID = str(data_qualities_csv['DataQualityID'][i])
        obj_data_quality.DataQualityTypeCV = data_qualities_csv['DataQualityTypeCV'][i]
        obj_data_quality.DataQualityCode = str(data_qualities_csv['DataQualityCode'][i])
        if not pd.isna(data_qualities_csv['DataQualityValue'][i]):
            obj_data_quality.DataQualityValue = str(data_qualities_csv['DataQualityValue'][i])
            obj_data_quality.DataQualityValueUnitsID = data_qualities_csv['DataQualityValueUnitsID'][i]
        if not pd.isna(data_qualities_csv['DataQualityDescription'][i]):
            obj_data_quality.DataQualityDescription = data_qualities_csv['DataQualityDescription'][i]
        if not pd.isna(data_qualities_csv['DataQualityLink'][i]):
            obj_data_quality.DataQualityLink = data_qualities_csv['DataQualityLink'][i]
        objects_data_qualities.append(obj_data_quality)
    except Exception as e:
        session.rollback()
        if obj_data_quality.DataQualityID is not None:
            print("issue loading single object %s: %s " % (obj_data_quality.DataQualityID, e))
        pass
session.add_all(objects_data_qualities)
if not args.debug:
    session.commit()

print('ODM2COL base structure Load has completed')
