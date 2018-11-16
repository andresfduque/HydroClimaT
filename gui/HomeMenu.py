#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
# Created by AndresD at 13/11/18

Features:
    + Manage database connections
        - Connect/Disconnect database (PostgreSQL, SQLite3)
    + Edit database
        - Create site
        - Create source
        - Create method
        - Create variable
    + Import data
        - IDEAM (Text-file, SIRH, DHIME)
        - SIATA (Future support)
        
    + Hydrology and climate time series management
    + Series processing, statistics and advanced analysis

@author:    Andres Felipe Duque Perez
Email:      andresfduque@gmail.com
"""
# TODO: add SIATA support
# TODO: add DHIME support

# %% Main imports
import os
import numpy as np
import pandas as pd
import ManageDatabases as myDb

from DatabaseCV import importCV
from DatabaseDeclarative import Base
from PyQt5.QtGui import QRegExpValidator
from odm2api.ODMconnection import dbconnection
from PyQt5.QtCore import pyqtSignal, Qt, QRegExp, pyqtSlot
from psycopg2.extensions import register_adapter, AsIs
from PyQt5.QtWidgets import (QComboBox, QHBoxLayout, QPushButton, QVBoxLayout, QWidget, QMessageBox, QTabWidget,
                             QMainWindow, QDockWidget, QStackedWidget, QLabel, QLineEdit, QFormLayout, QCheckBox, QSizePolicy)


# Main Workspace widget and database/time-series management
class HomeWidget(QMainWindow):
    def __init__(self, parent=None):
        super(HomeWidget, self).__init__(parent)

        # dock-widget for database management
        self.HLayout3 = QHBoxLayout()                   # layout for database connect/edit and database explore
        self.HLayout1 = QHBoxLayout()                   # layout for database selection
        self.HLayout2 = QHBoxLayout()                   # layout for buttons
        self.VLayout1 = QVBoxLayout()                   # final layout
        self.psqlWidget = QWidget()                     # database connection widget
        self.widget = QWidget()                         # widget defining dock-database widget
        self.connButton = QPushButton('Connect')        # connect to selected database
        self.delConnButton = QPushButton('Delete')      # delete connection parameters
        self.delConnButton.setEnabled(False)

        # set buttons layout
        self.HLayout2.addWidget(self.connButton)

        # set dock-widget for database connection
        self.postgresForm = PostgresForm()
        self.VLayout1.setAlignment(Qt.AlignTop)
        self.VLayout1.addWidget(self.postgresForm)
        self.VLayout1.addLayout(self.HLayout1)
        self.VLayout1.setContentsMargins(0, 6, 0, 0)
        self.widget.setLayout(self.VLayout1)

        self.dockDatabaseTitle = QLabel('Database connection')
        self.dockDatabaseTitle.setStyleSheet('background-color: rgb(158,162,170); border-radius: 3px; font:bold; '
                                             'font-size: 12px')
        self.dockDatabaseTitle.setContentsMargins(5, 5, 0, 5)

        self.dockDatabase = QDockWidget()
        self.dockDatabase.setWidget(self.widget)
        self.dockDatabase.setAllowedAreas(Qt.LeftDockWidgetArea)
        self.dockDatabase.setMinimumWidth(445)
        self.dockDatabase.setMinimumHeight(300)
        self.dockDatabase.setTitleBarWidget(self.dockDatabaseTitle)

        # dock-widget for database edition
        self.dockDbEditTitle = QLabel('Edit Database')
        self.dockDbEditTitle.setStyleSheet('background-color: rgb(158,162,170); border-radius: 3px; font:bold; '
                                           'font-size: 12px')
        self.dockDbEditTitle.setContentsMargins(5, 5, 0, 5)

        self.dockDbEdit = QDockWidget()
        self.dockDbEdit.setAllowedAreas(Qt.LeftDockWidgetArea)
        self.dockDbEdit.setMinimumWidth(445)
        self.dockDbEdit.setMinimumHeight(300)
        self.dockDbEdit.treeWidget = None
        self.dockDbEdit.setTitleBarWidget(self.dockDbEditTitle)

        # dock-widget for database explorer
        self.dockDbExploreTitle = QLabel('Database Explorer')
        self.dockDbExploreTitle.setStyleSheet('background-color: rgb(158,162,170); border-radius: 3px; font:bold; '
                                              'font-size: 12px')
        self.dockDbExploreTitle.setContentsMargins(5, 5, 0, 5)
        self.dockDbExplore = QDockWidget()
        self.dockDbExplore.setAllowedAreas(Qt.RightDockWidgetArea)
        self.dockDbExplore.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)

        self.dockDbExplore.setMinimumWidth(800)
        self.dockDbExplore.setSizePolicy(QSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding))
        self.dockDbExplore.setMinimumHeight(300)
        self.dockDbExplore.treeWidget = None
        self.dockDbExplore.setTitleBarWidget(self.dockDbExploreTitle)

        # stacked-widget for multiple database analysis
        self.stackedWorkspace = QStackedWidget()

        # assemble home widget
        self.setCentralWidget(self.stackedWorkspace)
        self.addDockWidget(Qt.LeftDockWidgetArea, self.dockDatabase)
        self.addDockWidget(Qt.LeftDockWidgetArea, self.dockDbEdit)
        self.addDockWidget(Qt.RightDockWidgetArea, self.dockDbExplore)

        self.setContentsMargins(5, 5, 5, 5)

        self.postgresForm.connDbSignal.connect(self.connDbSig)

    @pyqtSlot(object)
    def connDbSig(self):
        self.postgresForm.databaseLe.setEnabled(False)
        self.postgresForm.connNameLe.setEnabled(False)
        self.postgresForm.userLe.setEnabled(False)
        self.postgresForm.passLe.setEnabled(False)
        self.postgresForm.hostLe.setEnabled(False)
        self.postgresForm.portLe.setEnabled(False)
        self.postgresForm.serviceLe.setEnabled(False)
        self.postgresForm.userCheck.setEnabled(False)
        self.postgresForm.passCheck.setEnabled(False)
        self.postgresForm.connButton.setEnabled(False)
        self.postgresForm.delButton.setEnabled(False)
        self.postgresForm.newButton.setEnabled(False)


# %% Numpy sql adapter
# def adapt_numpy_int64(numpy_int64):
#     return AsIs(numpy_int64)


# %% Widget design for databases connections
# noinspection PyUnresolvedReferences
class PostgresForm(QWidget):
    # signals
    connDbSignal = pyqtSignal(object)   # send succesfull connection signal
    connDict = pyqtSignal(object)       # signal to pass dictionary to databases dock-widget

    def __init__(self, parent=None):
        super(PostgresForm, self).__init__(parent)

        # design of QDialog layout
        self.db_conn_widget = QWidget()
        self.dbForm = QFormLayout()                     # form for database connection information
        self.userForm = QFormLayout()                   # form for authentication
        self.userWidget = QWidget()                     # widget with user form layout - added to tabUser
        self.tabUser = QTabWidget()                     # tab-widget for database access authentication
        self.HLayout1 = QHBoxLayout()                   # layout for username credentials
        self.HLayout2 = QHBoxLayout()                   # layout for password credentials
        self.HLayout3 = QHBoxLayout()                   # layout for buttons
        self.VLayout = QVBoxLayout()                    # final layout

        self.dbCombobox = QComboBox()                   # combobox with connection names
        self.databaseLe = QLineEdit('odm_col')
        self.databaseLe = QLineEdit('odm_col')
        self.connNameLe = QLineEdit('postgres')
        self.userLe = QLineEdit('postgres')
        self.passLe = QLineEdit('postgres')
        self.passLe.setEchoMode(QLineEdit.Password)
        self.hostLe = QLineEdit('localhost')
        self.portLe = QLineEdit('5432')
        self.serviceLe = QLineEdit()
        self.dbCombobox.addItem('PostgreSQL')           # supported database
        self.dbCombobox.addItem('MySQL')                # TODO: add future support
        self.dbCombobox.addItem('SQLite3')              # TODO: add future support

        self.userCheck = QCheckBox('Save')
        self.passCheck = QCheckBox('Save')

        self.connButton = QPushButton('Connect database')
        self.newButton = QPushButton('New database')
        self.delButton = QPushButton('Delete database')

        # database connection
        self.dbForm.addRow('DB Engine', self.dbCombobox)
        self.dbForm.addRow('User', self.connNameLe)
        self.dbForm.addRow('Service', self.serviceLe)
        self.dbForm.addRow('Host', self.hostLe)
        self.dbForm.addRow('Port', self.portLe)
        self.dbForm.addRow('DB Name', self.databaseLe)

        self.HLayout1.addWidget(self.userLe)
        self.HLayout1.addWidget(self.userCheck)
        self.HLayout2.addWidget(self.passLe)
        self.HLayout2.addWidget(self.passCheck)
        self.HLayout3.addWidget(self.connButton)
        self.HLayout3.addWidget(self.newButton)
        self.HLayout3.addWidget(self.delButton)
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
        self.connButton.clicked.connect(self.connDB)
        self.newButton.clicked.connect(self.createDB)
        self.delButton.clicked.connect(self.delDB)

        self.regex1 = QRegExp("[0-9]+")
        self.validator1 = QRegExpValidator(self.regex1)
        self.regex2 = QRegExp("[0-9-a-z-A-Z_]+")
        self.validator2 = QRegExpValidator(self.regex2)
        self.portLe.setValidator(self.validator1)
        self.databaseLe.setValidator(self.validator2)
        self.connNameLe.setValidator(self.validator2)

    # connect to postgres database using ManageDatabases module
    def connDB(self):
        # connection parameters
        db_user = str(self.userLe.text())
        db_pass = str(self.passLe.text())
        db_host = str(self.hostLe.text())
        db_port = str(self.portLe.text())
        db_name = str(self.databaseLe.text())

        # connect database
        engine, meta, err = myDb.psql_conn(db_user, db_pass, db_name, db_host, db_port)

        if not err:
            psqlConnDict = {'user': db_user, 'password': db_pass, 'db_name': db_name,
                            'host': db_host, 'port': db_port, 'connName': self.connNameLe.text(),
                            'driver': 'postgres'}
            if '' not in psqlConnDict.values():
                QMessageBox.information(self, 'Database connection', 'Database connection added '
                                                                     'successfully', QMessageBox.Ok)

                # emit connection parameters to database toolbars, then to the database dock-widget
                self.connDbSignal.emit(1)
                self.connDict.emit(psqlConnDict)

            else:
                QMessageBox.critical(self, 'Database connection error', 'Make sure all requested '
                                                                        'fields are filled', QMessageBox.Ok)
        else:
            QMessageBox.critical(self, 'Database connection error', err, QMessageBox.Ok)

    # create postgres database using ManageDatabases module
    def createDB(self):
        # connection parameters
        dbUser = str(self.userLe.text())
        dbPass = str(self.passLe.text())
        dbHost = str(self.hostLe.text())
        dbPort = str(self.portLe.text())
        dbName = str(self.databaseLe.text())

        # create database
        engine, err = myDb.psql_create_db(dbUser, dbPass, dbName, dbHost, dbPort)

        if not err:
            session_factory = dbconnection.createConnection(engine='postgresql', address=dbHost, db=dbName, user=dbUser,
                                                            password=dbPass, dbtype=2.0, echo=True)
            assert session_factory is not None, ('failed to create a session for ', 'postgresql', dbHost)
            assert session_factory.engine is not None, ('failed: session has no engine ', 'postgresql', dbHost)
            s = session_factory.getSession()
            if 'postgresql' == 'postgresql':
                build = open(os.path.dirname(os.getcwd()) + '/schemas/postgresql/ODM2_for_PostgreSQL.sql').read()
                for line in build.split(';\n'):
                    s.execute(line)
                s.flush()
            s.commit()

            # importCV(Engine)  # import controlled vocabulary to database

            QMessageBox.information(self, 'Database creation', 'New POSTGRES database "' +
                                    dbName.upper() + '" created successfully', QMessageBox.Ok)
        else:
            QMessageBox.critical(self, 'Database creation error', err, QMessageBox.Ok)

    # delete postgres database using ManageDatabases module
    def delDB(self):
        # connection parameters
        dbUser = str(self.userLe.text())
        dbPass = str(self.passLe.text())
        dbHost = str(self.hostLe.text())
        dbPort = str(self.portLe.text())
        dbName = str(self.databaseLe.text())

        # connect database
        err = myDb.psql_drop_db(dbUser, dbPass, dbName, dbHost, dbPort)

        if not err:
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
            QMessageBox.critical(self, 'Database connection error', err, QMessageBox.Ok)

    # close dialog
    def closeDialog(self):
        self.close()


# %% Widget to visualize database
class DbExplorer(QWidget):
    def __init__(self, parent=None):
        super(DbExplorer, self).__init__(parent)

        self.label = QLabel('Explore Database')
        self.label.setStyleSheet('background-color: rgb(158,162,170); border-radius: 3px; font:bold; '
                                 'font-size: 12px')
        self.label.setContentsMargins(5, 5, 0, 5)

        self.VLayout = QVBoxLayout()
        self.HLayout = QHBoxLayout()

        self.subWindow = QMainWindow()
        self.dockGraphs = QDockWidget('Time-series')
        self.dockAuxGraphs = QDockWidget('Aux Graphics')
        self.dockStatistics = QDockWidget('Statistics')

        #        self.timeseriesPlot = pg.PlotWidget()
        #        self.dockGraphs.setWidget(self.timeseriesPlot)

        self.dockGraphs.setAllowedAreas(Qt.RightDockWidgetArea)
        self.dockAuxGraphs.setAllowedAreas(Qt.RightDockWidgetArea)
        self.dockStatistics.setAllowedAreas(Qt.RightDockWidgetArea)
        self.dockGraphs.setMinimumWidth(350)
        self.dockGraphs.setMinimumHeight(300)
        self.dockStatistics.setMinimumHeight(300)
        self.dockAuxGraphs.setMinimumHeight(300)

        self.tabWorkspace = QTabWidget()
        self.tabWorkspace.setMinimumWidth(250)

        self.subWindow.setCentralWidget(self.tabWorkspace)
        self.subWindow.addDockWidget(Qt.RightDockWidgetArea, self.dockGraphs)
        self.subWindow.addDockWidget(Qt.RightDockWidgetArea, self.dockAuxGraphs)
        self.subWindow.addDockWidget(Qt.RightDockWidgetArea, self.dockStatistics)
        self.subWindow.tabifyDockWidget(self.dockGraphs, self.dockAuxGraphs)
        self.dockGraphs.raise_()

        self.VLayout.addWidget(self.label)
        self.VLayout.addWidget(self.subWindow)

        self.VLayout.setContentsMargins(5, 0, 0, 0)

        self.setLayout(self.VLayout)

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
