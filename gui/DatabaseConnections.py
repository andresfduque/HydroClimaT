#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun 26 11:51:16 2017

DATABASES EDITOR:
    + Create and environmental database from template
        - create source
        - create site
        - create variable
        - create method
    + Import

REQUIREMENTS:
    + PostgreSQL 9.6 or SQLITE3
    + psycopg2 [python module]
    + SQL Alchemy [python module]

@author:    Andrés Felipe Duque Pérez
email:      andresfduque@gmail.com
"""

# %% Main imports

import os
import numpy as np
import pandas as pd
import ManageDatabases as myDb
from DatabaseCV import importCV
from DatabaseDeclarative import Base
from psycopg2.extensions import register_adapter, AsIs
from PyQt5.QtGui import QRegExpValidator
from PyQt5.QtCore import QRegExp, pyqtSignal, Qt
from PyQt5.QtWidgets import (QHBoxLayout, QLabel, QLineEdit, QPushButton, QVBoxLayout, QWidget, QMessageBox, QTabWidget,
                             QDialog, QFormLayout, QCheckBox)


# %% Numpy sql adapter
def adapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)


# %% Widget design for databases connections
# noinspection PyUnresolvedReferences
class PostgresForm(QDialog):
    # signal to pass dictionary to databases dock-widget
    connDict = pyqtSignal(object)

    def __init__(self, parent=None):
        super(PostgresForm, self).__init__(parent)

        # design of QDialog layout
        self.db_conn_widget = QWidget()
        self.dbFormTitle = QLabel('Connection information')
        self.dbFormTitle.setAlignment(Qt.AlignLeft)
        self.dbFormTitle.setStyleSheet('font:bold;')

        self.dbForm = QFormLayout()     # form for database connection information
        self.userForm = QFormLayout()   # form for authentication
        self.userWidget = QWidget()     # widget with user form layout - added to tabUser
        self.tabUser = QTabWidget()     # tab-widget for database access authentication
        self.HLayout1 = QHBoxLayout()   # layout for username credentials
        self.HLayout2 = QHBoxLayout()   # layout for password credentials
        self.HLayout3 = QHBoxLayout()   # layout for buttons
        self.VLayout = QVBoxLayout()    # final layout

        self.databaseLe = QLineEdit('odm_col')
        self.connNameLe = QLineEdit('postgres')
        self.userLe = QLineEdit('postgres')
        self.passLe = QLineEdit('postgres')
        self.passLe.setEchoMode(QLineEdit.Password)
        self.hostLe = QLineEdit('localhost')
        self.portLe = QLineEdit('5432')

        self.userCheck = QCheckBox('Save')
        self.passCheck = QCheckBox('Save')

        self.connButton = QPushButton('Add connection')
        self.newButton = QPushButton('New database')
        self.delButton = QPushButton('Delete database')
        self.cancelButton = QPushButton('Close')

        # database connection
        self.dbForm.addRow(self.dbFormTitle)
        self.dbForm.addRow('Name', self.connNameLe)
        self.dbForm.addRow('Service', QLineEdit())
        self.dbForm.addRow('Host', self.hostLe)
        self.dbForm.addRow('Port', self.portLe)
        self.dbForm.addRow('Database', self.databaseLe)

        self.HLayout1.addWidget(self.userLe)
        self.HLayout1.addWidget(self.userCheck)
        self.HLayout2.addWidget(self.passLe)
        self.HLayout2.addWidget(self.passCheck)
        self.HLayout3.addWidget(self.connButton)
        self.HLayout3.addWidget(self.newButton)
        self.HLayout3.addWidget(self.delButton)
        self.HLayout3.addWidget(self.cancelButton)
        self.userForm.addRow('Username', self.HLayout1)
        self.userForm.addRow('Password', self.HLayout2)

        self.userWidget.setLayout(self.userForm)
        self.tabUser.addTab(self.userWidget, 'Authentication')

        self.VLayout.addLayout(self.dbForm)
        self.VLayout.addWidget(self.tabUser)
        self.VLayout.addLayout(self.HLayout3)

        self.setLayout(self.VLayout)

        self.setMinimumWidth(400)
        self.setWindowTitle('Create a New Postgres Connection')
        self.connButton.clicked.connect(self.connPostgresDB)
        self.newButton.clicked.connect(self.createPostgresDB)
        self.delButton.clicked.connect(self.delPostgresDB)
        self.cancelButton.clicked.connect(self.closeDialog)

        self.regex1 = QRegExp("[0-9]+")
        self.validator1 = QRegExpValidator(self.regex1)
        self.regex2 = QRegExp("[0-9-a-z-A-Z_]+")
        self.validator2 = QRegExpValidator(self.regex2)
        self.portLe.setValidator(self.validator1)
        self.databaseLe.setValidator(self.validator2)
        self.connNameLe.setValidator(self.validator2)

    # connect to postgres database using ManageDatabases module
    def connPostgresDB(self):
        # connection parameters
        db_user = str(self.userLe.text())
        db_pass = str(self.passLe.text())
        db_host = str(self.hostLe.text())
        db_port = str(self.portLe.text())
        db_name = str(self.databaseLe.text())

        # connect database
        Engine, Meta, Err = myDb.psql_conn(db_user, db_pass, db_name, db_host, db_port)

        if not Err:
            psqlConnDict = {'user': db_user, 'password': db_pass, 'db_name': db_name,
                            'host': db_host, 'port': db_port, 'connName': self.connNameLe.text(),
                            'driver': 'postgres'}
            if '' not in psqlConnDict.values():
                QMessageBox.information(self, 'Database connection', 'Database connection added '
                                                                     'successfully', QMessageBox.Ok)

                # emit connection parameters to database toolbars, then to the database dock-widget
                self.connDict.emit(psqlConnDict)

            else:
                QMessageBox.critical(self, 'Database connection error', 'Make sure all requested '
                                                                        'fields are filled', QMessageBox.Ok)
        else:
            QMessageBox.critical(self, 'Database connection error', Err, QMessageBox.Ok)

    # create postgres database using ManageDatabases module
    def createPostgresDB(self):
        # connection parameters
        dbUser = str(self.userLe.text())
        dbPass = str(self.passLe.text())
        dbHost = str(self.hostLe.text())
        dbPort = str(self.portLe.text())
        dbName = str(self.databaseLe.text())

        # create database
        Engine, Err = myDb.psql_create_db(dbUser, dbPass, dbName, dbHost, dbPort)

        if not Err:
            psqlConnDict = {'user': dbUser, 'password': dbPass, 'dbname': dbName,
                            'host': dbHost, 'port': dbPort, 'connName': self.connNameLe.text(),
                            'driver': 'postgres'}
            if '' not in psqlConnDict.values():
                Engine, Meta, Err = myDb.psql_conn(dbUser, dbPass, dbName, dbHost, dbPort)
                Base.metadata.create_all(Engine)

                importCV(Engine)  # import controlled vocabulary to database

                QMessageBox.information(self, 'Database creation', 'New POSTGRES database "' +
                                        dbName.upper() + '" created successfully', QMessageBox.Ok)
                reply = QMessageBox.question(self, 'Format database', 'Format database to fit IDEAM data?',
                                             QMessageBox.Yes | QMessageBox.No)
                # format database to fit IDEAM datastructure (optional)structure
                if reply == QMessageBox.Yes:
                    formatIdeamDatabase(Engine)
                    QMessageBox.information(self, 'Database structure', '"' + dbName.upper() +
                                            '" database has been structured to fit IDEAM data', QMessageBox.Ok)
            else:
                QMessageBox.critical(self, 'Database connection error', 'Make sure all requested '
                                                                        'fields are filled', QMessageBox.Ok)
        else:
            QMessageBox.critical(self, 'Database creation error', Err, QMessageBox.Ok)

    # delete postgres database using ManageDatabases module
    def delPostgresDB(self):
        # connection parameters
        dbUser = str(self.userLe.text())
        dbPass = str(self.passLe.text())
        dbHost = str(self.hostLe.text())
        dbPort = str(self.portLe.text())
        dbName = str(self.databaseLe.text())

        # connect database
        Err = myDb.psql_drop_db(dbUser, dbPass, dbName, dbHost, dbPort)

        if not Err:
            psqlConnDict = {'user': dbUser, 'password': dbPass, 'dbname': dbName,
                            'host': dbHost, 'port': dbPort, 'connName': self.connNameLe.text(),
                            'driver': 'postgres'}
            if '' not in psqlConnDict.values():
                QMessageBox.information(self, 'Database deletion', 'Database "' + dbName.upper() +
                                        '" deleted successfully', QMessageBox.Ok)

            else:
                QMessageBox.critical(self, 'Database connection error', 'Make sure all requested '
                                                                        'fields are filled', QMessageBox.Ok)
        else:
            QMessageBox.critical(self, 'Database connection error', Err, QMessageBox.Ok)

    # close dialog
    def closeDialog(self):
        self.close()


def formatIdeamDatabase(engine):
    register_adapter(np.int64, adapt_numpy_int64)

    local_dir = os.getcwd()
    resources_dir = local_dir.split('/')
    resources_dir = '/'.join(resources_dir[:-1]) + '/resources/database_structure/'
    conn = engine.connect()

    metadata_default_filepath = resources_dir + 'Metadata/ISOMetadata.xlsx'
    sources_default_filepath = resources_dir + 'Sources/Sources.xlsx'
    variables_default_filepath = resources_dir + 'Variables/Variables.xlsx'
    qualifiers_default_filepath = resources_dir + 'Qualifiers/Qualifiers.xlsx'
    methods_default_filepath = resources_dir + 'Methods/Methods.xlsx'
    qualities_default_filepath = resources_dir + 'Quality Control Levels/Quality.xlsx'
    sites_IDEAM_Met_default_filepath = resources_dir + 'Sites/IDEAM_MET_V2.csv'
    sites_IDEAM_Hid_default_filepath = resources_dir + 'Sites/IDEAM_HID_V2.csv'
    sites_Otras_Met_default_filepath = resources_dir + 'Sites/OTRAS_MET_V2.xlsx'
    sites_Otras_Hid_default_filepath = resources_dir + 'Sites/OTRAS_HID_V2.xlsx'

    # create metadata
    dataframe = pd.read_excel(metadata_default_filepath)
    for i in dataframe.index:
        metaId = int(dataframe['ID'][i])
        category = dataframe['Categoria'][i]
        title = dataframe['Titulo'][i]
        abstract = dataframe['Resumen'][i]
        profiler = dataframe['Profiler Version'][i]
        link = dataframe['Link'][i]

        conn.execute('INSERT INTO "ISOMetadata" ("MetadataId", "TopicCategory", "Title", '
                     '"Abstract", "ProfileVersion", "MetadataLink") VALUES (%s, %s, %s, %s, %s, %s)',
                     (metaId, category, title, abstract, profiler, link))

    # create sources
    dataframe = pd.read_excel(sources_default_filepath)
    for i in dataframe.index:
        metadata = dataframe['MetadataID'][i]
        organization = dataframe['Organization'][i]
        description = dataframe['Description'][i]
        link = dataframe['Link'][i]
        contact = dataframe['Contact'][i]
        phone = dataframe['Phone'][i]
        email = dataframe['Email'][i]
        address = dataframe['Address'][i]
        state = dataframe['State'][i]
        city = dataframe['City'][i]
        zipCode = dataframe['Zipcode'][i]
        citation = dataframe['Citation'][i]

        conn.execute('INSERT INTO "Sources" ("Organization", "SourceDescription", "SourceLink", "ContactName", '
                     '"Phone", "Email", "Address", "City", "State", "ZipCode", "Citation", "MetadataId") '
                     'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                     (organization, description, link, contact, phone, email, address, city, state, zipCode,
                      citation, metadata))

    # create variables
    dataframe = pd.read_excel(variables_default_filepath)
    for i in dataframe.index:
        varId = dataframe['Code'][i]
        code = dataframe['Code'][i]
        name = dataframe['Name'][i]
        speciation = dataframe['Speciation'][i]
        vUnitsId = dataframe['VarUnits'][i]
        sampleMedium = dataframe['SampleMedium'][i]
        vType = dataframe['ValueType'][i]
        isRegular = dataframe['IsRegular'][i]
        if isRegular == 1:
            isRegular = True
        else:
            isRegular = False
        tSupport = dataframe['TimeSupport'][i]
        tUnitsId = dataframe['TimeUnits'][i]
        dType = dataframe['DataType'][i]
        category = dataframe['GeneralCategory'][i]
        noData = dataframe['NoData'][i]

        conn.execute('INSERT INTO "Variables" ("VariableId", "VariableCode", "VariableName", "Speciation", '
                     '"VariableUnitsId", "SampleMedium", "ValueType", "IsRegular", "TimeSupport", "TimeUnitsId", '
                     '"DataType", "GeneralCategory", "NoDataValue") '
                     'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                     (varId, code, name, speciation, vUnitsId, sampleMedium, vType, isRegular, tSupport, tUnitsId,
                      dType, category, noData))

    # create qualifiers
    dataframe = pd.read_excel(qualifiers_default_filepath)
    for i in dataframe.index:
        qualId = int(dataframe['ID'][i])
        code = dataframe['Code'][i]
        description = dataframe['Description'][i]

        conn.execute('INSERT INTO "Qualifiers" ("QualifierId", "QualifierCode", "QualifierDescription") '
                     'VALUES (%s, %s, %s)', (qualId, code, description))

    # create methods
    dataframe = pd.read_excel(methods_default_filepath)
    for i in dataframe.index:
        code = dataframe['ID'][i]
        description = dataframe['Description'][i]
        link = dataframe['Link'][i]

        conn.execute('INSERT INTO "Methods" ("MethodId", "MethodDescription", "MethodLink") VALUES (%s, %s, %s)',
                     (code, description, link))

    # create qualities
    dataframe = pd.read_excel(qualities_default_filepath)
    for i in dataframe.index:
        qualId = int(dataframe['ID'][i])
        code = dataframe['Code'][i]
        definition = dataframe['Description'][i]
        explanation = dataframe['Explanation'][i]

        conn.execute('INSERT INTO "QualityControlLevels" ("QualityControlLevelId", "QualityControlLevelCode", '
                     '"Definition", "Explanation") VALUES (%s, %s, %s, %s)', (qualId, code, definition, explanation))

    # create IDEAM hydrologic sites
    dataframe = pd.read_csv(sites_IDEAM_Hid_default_filepath)
    for i in dataframe.index:
        siteId = np.int(dataframe['COD_CATALOGO_ES'][i])
        siteCode = dataframe['COD_CATALOGO_ES'][i]
        siteName = dataframe['NOMBRE_ES'][i]
        siteLatitude = float(dataframe['LATITUD'][i])
        siteLongitude = float(dataframe['LONGITUD'][i])
        siteState = dataframe['DEPARTAMENTO'][i]
        siteCity = dataframe['MUNICIPIO'][i]

        conn.execute('INSERT INTO "Sites" ("SiteId", "SiteCode", "SiteName", "Latitude", "Longitude", '
                     '"LatLongDatumId", "State", "County", "SiteType") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)',
                     (siteId, siteCode, siteName, siteLatitude, siteLongitude, 1, siteState, siteCity,
                      'Stream'))

    # create "Other" hydrologic sites
    dataframe = pd.read_excel(sites_Otras_Hid_default_filepath)
    for i in dataframe.index:
        siteId = np.int(dataframe['COD_CATALOGO_ES'][i])
        siteCode = dataframe['COD_CATALOGO_ES'][i]
        siteName = dataframe['NOMBRE_ES'][i]
        siteLatitude = float(dataframe['LATITUD'][i])
        siteLongitude = float(dataframe['LONGITUD'][i])
        siteState = dataframe['DEPARTAMENTO'][i]
        siteCity = dataframe['MUNICIPIO'][i]

        conn.execute('INSERT INTO "Sites" ("SiteId", "SiteCode", "SiteName", "Latitude", "Longitude", '
                     '"LatLongDatumId", "State", "County", "SiteType") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)',
                     (siteId, siteCode, siteName, siteLatitude, siteLongitude, 1, siteState, siteCity,
                      'Stream'))

    # create IDEAM climate sites
    dataframe = pd.read_csv(sites_IDEAM_Met_default_filepath)
    for i in dataframe.index:
        siteId = np.int(dataframe['COD_CATALOGO_ES'][i])
        siteCode = dataframe['COD_CATALOGO_ES'][i]
        siteName = dataframe['NOMBRE_ES'][i]
        siteLatitude = float(dataframe['LATITUD'][i])
        siteLongitude = float(dataframe['LONGITUD'][i])
        siteState = dataframe['DEPARTAMENTO'][i]
        siteCity = dataframe['MUNICIPIO'][i]

        conn.execute('INSERT INTO "Sites" ("SiteId", "SiteCode", "SiteName", "Latitude", "Longitude", '
                     '"LatLongDatumId", "State", "County", "SiteType") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)',
                     (siteId, siteCode, siteName, siteLatitude, siteLongitude, 1, siteState, siteCity,
                      'Atmosphere'))

    # create "Other" climate sites
    dataframe = pd.read_excel(sites_Otras_Met_default_filepath)
    for i in dataframe.index:
        siteId = np.int(dataframe['COD_CATALOGO_ES'][i])
        siteCode = dataframe['COD_CATALOGO_ES'][i]
        siteName = dataframe['NOMBRE_ES'][i]
        siteLatitude = float(dataframe['LATITUD'][i])
        siteLongitude = float(dataframe['LONGITUD'][i])
        siteState = dataframe['DEPARTAMENTO'][i]
        siteCity = dataframe['MUNICIPIO'][i]

        conn.execute('INSERT INTO "Sites" ("SiteId", "SiteCode", "SiteName", "Latitude", "Longitude", '
                     '"LatLongDatumId", "State", "County", "SiteType") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)',
                     (siteId, siteCode, siteName, siteLatitude, siteLongitude, 1, siteState, siteCity,
                      'Atmosphere'))
