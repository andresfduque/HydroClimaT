#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jul 13 18:41:25 2017

DATABASE EDITOR:
    + Explore database
        - Get sites, timeseries, variables, methods, sources, quality control levels and qualifiers
    + Edit database
        - Edit, add and delete sites
        - Edit, add and delete variables
        - Edit, add and delete methods
        - Edit, add and delete sources
        - Edit, add and delete quality control levels
        - Edit, add and delete qualifiers

REQUIREMENTS:
    + PostgreSQL 10.1 or SQLITE3
    + psycopg2 [python module]
    + SQL Alchemy [python module]

@author:    Andrés Felipe Duque Pérez
email:      andresfduque@gmail.com
"""

# %% Main imports
import numpy as np
import pandas as pd
from ImportSeries import exploreIdeamMultipleFiles as expIDEAMFiles
from ImportSeries import importIdeamDailyTxt as importIDEAMDaily
import SQLAlchemyQueries as SqlQuery
from sqlalchemy.orm import sessionmaker
from PyQt5.QtCore import pyqtSignal, QRegExp
from PyQt5.QtGui import QFont, QRegExpValidator
from sqlalchemy.exc import DataError, IntegrityError
from psycopg2.extensions import register_adapter, AsIs
from PyQt5.QtWidgets import (QComboBox, QDialog, QLabel, QGridLayout, QVBoxLayout, QPushButton, QRadioButton,
                             QHBoxLayout, QLineEdit, QMessageBox, QFileDialog, QListWidget, QSpinBox, QGroupBox)
from DatabaseDeclarative import (Base, Sources, Sites, ISOMetadata, Variables, Qualifiers, Methods,
                                 QualityControlLevels)


# %% Start DBSession
def startDBSession(engine):

    # Bind the engine to the metadata of the Base class so that the
    # declarative can be accessed through a DBSession instance
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


# %% Numpy sql adapter
def adapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)


# %% Combo-box that add all items of a list
class CboxList(QComboBox):
    def __init__(self, elements_list, nullable=False, parent=None):
        super(CboxList, self).__init__(parent)
        if nullable:
            self.addItem('<none>')
        for i in elements_list:
            self.addItem(str(i))


# %% File dialog to get csv or excel filename
class CsvFileDialog(QFileDialog):
    def __init__(self, parent=None):
        super(CsvFileDialog, self).__init__(parent)
        self.setNameFilters(["CSV files (*.csv)", "XLS files (*.xls *.xlsx)"])
        self.selectNameFilter("CSV files (*.csv);;XLS files (*.xls *.xlsx)")
#        self.setFileMode(QFileDialog.ExistingFile)


# %% Editable text box that set entered values into CAPS
class EditableTextBox(QLineEdit):

    def __init__(self, *args):
        # args to set parent
        QLineEdit.__init__(self, *args)

    # focusOutEvent
    def focusOutEvent(self, *args, **kwargs):
        text = self.text()
        self.setText(text.__str__().upper())
        return QLineEdit.focusOutEvent(self, *args, **kwargs)

    # keyPressEvent
    def keyPressEvent(self, event):
        if not self.hasSelectedText():
            pretext = self.text()
            self.setText(pretext.__str__().upper())
        return QLineEdit.keyPressEvent(self, event)

    # keyPressEvent
    def keyReleaseEvent(self, event):
        if not self.hasSelectedText():
            pretext = self.text()
            self.setText(pretext.__str__().upper())
        return QLineEdit.keyReleaseEvent(self, event)


# %% Site explorer form
# noinspection PyUnresolvedReferences
class DbExplorer(QDialog):

    # signal to pass dictionary to databases dock-widget
    timeSeriesIdentifier = pyqtSignal(object)

    def __init__(self, workspace_site, engine, parent=None):
        super(DbExplorer, self).__init__(parent)

        descLabelFont = QFont()
        descLabelFont.setPointSize(5)

        # main layout
        self.VLayout = QVBoxLayout()
        self.HLayout = QHBoxLayout()
        self.Grid = QGridLayout()

        # labels
        self.sourcesLb = QLabel('Sources')
        self.varsLb = QLabel('Variables')
        self.methodsLb = QLabel('Methods')
        self.qualitiesLb = QLabel('Quality control levels')
        self.qualitiesDescLb = QLabel()
        self.qualitiesDescLb.setFont(descLabelFont)

        # buttons
        self.addSeriesButton = QPushButton('Add time series')
        self.closeButton = QPushButton('Close')
        self.closeButton.clicked.connect(self.closeDialog)

        # comb-boxes
        self.sourcesCb = QComboBox()
        self.varsCb = QComboBox()
        self.methodsCb = QComboBox()
        self.qualitiesCb = QComboBox()

        # set layout
        self.Grid.addWidget(self.sourcesLb, 0, 0)
        self.Grid.addWidget(self.varsLb, 1, 0)
        self.Grid.addWidget(self.methodsLb, 2, 0)
        self.Grid.addWidget(self.qualitiesLb, 3, 0)
        self.Grid.addWidget(self.qualitiesDescLb, 4, 1)

        self.Grid.addWidget(self.sourcesCb, 0, 1)
        self.Grid.addWidget(self.varsCb, 1, 1)
        self.Grid.addWidget(self.methodsCb, 2, 1)
        self.Grid.addWidget(self.qualitiesCb, 3, 1)

        self.HLayout.addWidget(self.addSeriesButton)
        self.HLayout.addWidget(self.closeButton)

        self.VLayout.addLayout(self.Grid)
        self.VLayout.addLayout(self.HLayout)

        self.setLayout(self.VLayout)
        self.setWindowTitle('Station code: ' + workspace_site[0])

        # get site data
        self.siteQueryDictionary = SqlQuery.sitesQuery(int(workspace_site[0]), engine)

        # add data to form
        siteSourcesName = self.siteQueryDictionary['sourcesName']
        siteSourcesId = self.siteQueryDictionary['sourcesId']

        siteVariablesName = self.siteQueryDictionary['variablesName']
        siteVariablesId = self.siteQueryDictionary['variablesId']

        siteMethodsName = self.siteQueryDictionary['methodsName']
        siteMethodsId = self.siteQueryDictionary['methodsId']

        siteQualitiesName = self.siteQueryDictionary['qualitiesName']
        siteQualitiesId = self.siteQueryDictionary['qualitiesId']

        for i in range(len(siteSourcesName)):
            if siteSourcesId[i] >= 10:
                self.sourcesCb.addItem('[' + str(siteSourcesId[i]) + '] ' + siteSourcesName[i])
            else:
                self.sourcesCb.addItem('[0' + str(siteSourcesId[i]) + '] ' + siteSourcesName[i])
        for i in range(len(siteMethodsName)):
            if siteMethodsId[i] >= 10:
                self.methodsCb.addItem('[' + str(siteMethodsId[i]) + '] ' + siteMethodsName[i])
            else:
                self.methodsCb.addItem('[0' + str(siteMethodsId[i]) + '] ' + siteMethodsName[i])
        for i in range(len(siteQualitiesName)):
            if siteQualitiesId[i] >= 10:
                self.qualitiesCb.addItem('[' + str(siteQualitiesId[i]) + '] ' +
                                         siteQualitiesName[i])
            else:
                self.qualitiesCb.addItem('[0' + str(siteQualitiesId[i]) + '] ' +
                                         siteQualitiesName[i])
        for i in range(len(siteVariablesName)):
            if siteVariablesId[i] >= 10:
                self.varsCb.addItem('[' + str(siteVariablesId[i]) + '] ' + siteVariablesName[i])
            else:
                self.varsCb.addItem('[0' + str(siteVariablesId[i]) + '] ' + siteVariablesName[i])

        if self.sourcesCb.currentText() == '':
            self.addSeriesButton.setEnabled(False)

        # add timeseries to workspace
        self.addSeriesButton.clicked.connect(self.emitTsIdentifier)

        # call dialog box
        self.show()

    def emitTsIdentifier(self):
        source = self.sourcesCb.currentText()
        variable = self.varsCb.currentText()
        method = self.methodsCb.currentText()
        quality = self.qualitiesCb.currentText()

        if source != '':
            # signal to pass time-series identifier to main-window
            self.timeSeriesIdentifier.emit([source, variable, method, quality])

        self.closeDialog()

    def closeDialog(self):
        self.close()


# %% Add Metadata form
# noinspection PyUnresolvedReferences
class AddMetadata(QDialog):

    def __init__(self, engine, parent=None):
        super(AddMetadata, self).__init__(parent)

        self.engine = engine
        self.setMinimumWidth(250)

        # main layout
        self.VLayout = QVBoxLayout()
        self.HLayout = QHBoxLayout()
        self.Grid = QGridLayout()

        # labels
        self.idLb = QLabel('ID')
        self.categoryLb = QLabel('Topic category')
        self.titleLb = QLabel('Title')
        self.abstractLb = QLabel('Abstract')
        self.profilerVersionLb = QLabel('Profiler version')
        self.linkLb = QLabel('Metadata link')

        # line edits
        self.idLe = QLineEdit()
        self.idLe.setMaxLength(8)
        self.regex1 = QRegExp("[0-9]+")
        self.validator1 = QRegExpValidator(self.regex1)
        self.idLe.setValidator(self.validator1)

        self.titleLe = QLineEdit()
        self.abstractLe = QLineEdit()
        self.profilerVersionLe = QLineEdit()
        self.linkLe = QLineEdit()

        # comb-boxes
        self.categoryCb = QComboBox()

        # buttons
        self.addMetadataButton = QPushButton('Add Metadata')
        self.addMetadataMultipleButton = QPushButton('Import Metadata...')
        self.closeButton = QPushButton('Close')
        self.closeButton.clicked.connect(self.closeDialog)
        self.addMetadataButton.clicked.connect(self.createMetadataSingle)
        self.addMetadataMultipleButton.clicked.connect(self.createMultipleMetadata)

        # set layout
        self.Grid.addWidget(self.idLb, 0, 0)
        self.Grid.addWidget(self.categoryLb, 1, 0)
        self.Grid.addWidget(self.titleLb, 2, 0)
        self.Grid.addWidget(self.abstractLb, 3, 0)
        self.Grid.addWidget(self.profilerVersionLb, 4, 0)
        self.Grid.addWidget(self.linkLb, 5, 0)

        self.Grid.addWidget(self.idLe, 0, 1)
        self.Grid.addWidget(self.categoryCb, 1, 1)
        self.Grid.addWidget(self.titleLe, 2, 1)
        self.Grid.addWidget(self.abstractLe, 3, 1)
        self.Grid.addWidget(self.profilerVersionLe, 4, 1)
        self.Grid.addWidget(self.linkLe, 5, 1)

        self.HLayout.addWidget(self.addMetadataButton)
        self.HLayout.addWidget(self.addMetadataMultipleButton)
        self.HLayout.addWidget(self.closeButton)

        self.VLayout.addLayout(self.Grid)
        self.VLayout.addLayout(self.HLayout)

        self.setLayout(self.VLayout)
        self.setMinimumWidth(500)
        self.setWindowTitle('Create new ISO metadata')

        # get metadata CV data (topic category)
        self.topicCategoryDictionary = SqlQuery.get_topicCategory_table(engine)

        # add data to form
        topicCategory = self.topicCategoryDictionary['Term']
        for i in topicCategory:
            self.categoryCb.addItem(i)

        # call dialog
        self.exec_()

    # close dialog
    def closeDialog(self):
        self.close()

    # add metadata
    def createMetadataSingle(self):
        session = startDBSession(self.engine)

        metadataId = self.idLe.text()
        metadataCategory = self.categoryCb.currentText()
        metadataTitle = self.titleLe.text()
        metadataAbstract = self.abstractLe.text()
        metadataProfiler = self.profilerVersionLe.text()
        metadataLink = self.linkLe.text()

        if metadataTitle == '':
            metadataTitle = 'Unknown'
        if metadataAbstract == '':
            metadataAbstract = 'Unknown'
        if metadataProfiler == '':
            metadataProfiler = 'Unknown'
        if metadataLink == '':
            metadataLink = 'Unknown'

        # add metadata
        try:
            metadata = ISOMetadata(MetadataId=metadataId,
                                   TopicCategory=metadataCategory,
                                   Title=metadataTitle,
                                   Abstract=metadataAbstract,
                                   ProfileVersion=metadataProfiler,
                                   MetadataLink=metadataLink)
            session.add(metadata)
            session.commit()
            QMessageBox.information(self, 'Database edition', 'Metadata definition created!',
                                    QMessageBox.Ok)
        except DataError:
            session.rollback()
            QMessageBox.critical(self, 'Database edition error', "Metadata couldn't be added, "
                                 "make sure provided data fits Controlled Vocabulary (CV) or "
                                 "input types", QMessageBox.Ok)

    def createMultipleMetadata(self):
        # read .csv or excel file with sites data
        fileName, _ = CsvFileDialog.getOpenFileName(self)
        if fileName:
            if fileName[-3:] == 'csv':
                metaDF = pd.read_csv(fileName)
            else:
                metaDF = pd.read_excel(fileName)

            # add multiple sites dialog
            AddMultipleMetadata(self.engine, metaDF)


# %% Add multiple metadata [form]
# noinspection PyUnresolvedReferences
class AddMultipleMetadata(QDialog):
    def __init__(self, engine, meta_df, parent=None):
        super(AddMultipleMetadata, self).__init__(parent)

        self.engine = engine
        self.metaDF = meta_df
        self.columns = list(self.metaDF)

        # main layout
        self.VLayout = QVBoxLayout()
        self.VLayout1 = QVBoxLayout()
        self.HLayout = QHBoxLayout()
        self.Grid = QGridLayout()

        # labels
        self.idLb = QLabel('ID')
        self.categoryLb = QLabel('Topic category')
        self.titleLb = QLabel('Title')
        self.abstractLb = QLabel('Abstract')
        self.profilerVersionLb = QLabel('Profiler version')
        self.linkLb = QLabel('Metadata link')

        # combo-boxes
        self.idCb = CboxList(self.columns)
        self.categoryCb = CboxList(self.columns)
        self.titleCb = CboxList(self.columns)
        self.abstractCb = CboxList(self.columns)
        self.profilerVersionCb = CboxList(self.columns)
        self.linkCb = CboxList(self.columns)

        # buttons
        self.importMetasBttn = QPushButton('Import ISO metadata')
        self.closeBttn = QPushButton('Close')
        self.importMetasBttn.clicked.connect(self.importMultipleMetas)
        self.closeBttn.clicked.connect(self.closeDialog)

        # set layout
        self.Grid.addWidget(self.idLb, 0, 0)
        self.Grid.addWidget(self.categoryLb, 1, 0)
        self.Grid.addWidget(self.titleLb, 2, 0)
        self.Grid.addWidget(self.abstractLb, 3, 0)
        self.Grid.addWidget(self.profilerVersionLb, 4, 0)
        self.Grid.addWidget(self.linkLb, 5, 0)

        self.Grid.addWidget(self.idCb, 0, 1)
        self.Grid.addWidget(self.categoryCb, 1, 1)
        self.Grid.addWidget(self.titleCb, 2, 1)
        self.Grid.addWidget(self.abstractCb, 3, 1)
        self.Grid.addWidget(self.profilerVersionCb, 4, 1)
        self.Grid.addWidget(self.linkCb, 5, 1)

        self.HLayout.addWidget(self.importMetasBttn)
        self.HLayout.addWidget(self.closeBttn)

        self.VLayout.addLayout(self.Grid)
        self.VLayout.addLayout(self.HLayout)

        self.setLayout(self.VLayout)
        self.setMinimumWidth(500)
        self.setWindowTitle('Create multiple new ISO metadata')

        # call dialog
        self.exec_()

    # close
    def closeDialog(self):
        self.close()

    # import sites data
    def importMultipleMetas(self):
        session = startDBSession(self.engine)
        register_adapter(np.int64, adapt_numpy_int64)

        # assign variables to data-frame columns
        idCol = self.idCb.currentText()
        categoryCol = self.categoryCb.currentText()
        titleCol = self.titleCb.currentText()
        abstractCol = self.abstractCb.currentText()
        profilerCol = self.profilerVersionCb.currentText()
        linkCol = self.linkCb.currentText()

        # add each metadata
        try:
            for i in self.metaDF.index:
                metaId = int(self.metaDF[idCol][i])
                category = self.metaDF[categoryCol][i]
                title = self.metaDF[titleCol][i]
                abstract = self.metaDF[abstractCol][i]
                profiler = self.metaDF[profilerCol][i]
                link = self.metaDF[linkCol][i]

                metadata = ISOMetadata(MetadataId=metaId,
                                       TopicCategory=category,
                                       Title=title,
                                       Abstract=abstract,
                                       ProfileVersion=profiler,
                                       MetadataLink=link)
                session.add(metadata)

            session.commit()
            QMessageBox.information(self, 'Database edition', 'Multiple ISO metadata created!',
                                    QMessageBox.Ok)
        except DataError:
            session.rollback()
            QMessageBox.critical(self, 'Database edition error [DataError]', "ISO metadata couldn't be added, "
                                 "make sure provided data fits Controlled Vocabulary (CV) or "
                                 "input types", QMessageBox.Ok)
        except IntegrityError:
            session.rollback()
            QMessageBox.critical(self, 'Database edition error [IntegrityError]', "ISO metadata couldn't be added, "
                                 "make sure provided data fits Controlled Vocabulary (CV) or "
                                 "input types", QMessageBox.Ok)


# %% Add new site [form]
# noinspection PyUnresolvedReferences
class AddSite(QDialog):
    def __init__(self, engine, parent=None):
        super(AddSite, self).__init__(parent)

        self.engine = engine

        # main layout
        self.VLayout = QVBoxLayout()
        self.VLayout1 = QVBoxLayout()
        self.HLayout = QHBoxLayout()
        self.Grid = QGridLayout()

        # labels
        self.codeLb = QLabel('Id / Code')
        self.nameLb = QLabel('Name')
        self.siteTypeLb = QLabel('Type')
        self.latitudeLb = QLabel('Latitude')
        self.longitudeLb = QLabel('Longitude')
        self.latlonDatumLb = QLabel('Lat / Lon datum')
        self.latlonVDatumLb = QLabel('Lat / Lon vertical datum')

        self.localxLb = QLabel('Local X')
        self.localyLb = QLabel('Local Y')
        self.localProjLb = QLabel('Local Projection')
        self.posAccLb = QLabel('Position Accuracy')
        self.departmentLb = QLabel('State')
        self.municipalityLb = QLabel('City')
        self.commentsLb = QLabel('Comments')

        # line edits
        self.codeLe = QLineEdit()
        self.codeLe.setMaxLength(8)
        self.regex1 = QRegExp("[0-9]+")
        self.validator1 = QRegExpValidator(self.regex1)
        self.codeLe.setValidator(self.validator1)

        self.nameLe = EditableTextBox()
        self.nameLe.setMaxLength(20)
        self.regex2 = QRegExp("[0-9-a-z-A-Z\s]+")
        self.validator2 = QRegExpValidator(self.regex2)
        self.nameLe.setValidator(self.validator2)

        self.latitudeLe = QLineEdit()
        self.latitudeLe.setMaxLength(9)
        self.regex3 = QRegExp("[-+]?([0-9]*\.[0-9]+|[0-9]+)")
        self.validator3 = QRegExpValidator(self.regex3)
        self.latitudeLe.setValidator(self.validator3)

        self.longitudeLe = QLineEdit()
        self.longitudeLe.setMaxLength(9)
        self.longitudeLe.setValidator(self.validator3)

        self.localxLe = QLineEdit()
        self.localyLe = QLineEdit()
        self.localxLe.setMaxLength(11)
        self.localyLe.setMaxLength(11)
        self.localxLe.setValidator(self.validator3)
        self.localyLe.setValidator(self.validator3)

        self.posAccLe = QLineEdit()
        self.posAccLe.setMaxLength(5)
        self.posAccLe.setValidator(self.validator3)

        self.departmentLe = EditableTextBox()
        self.municipalityLe = EditableTextBox()
        self.departmentLe.setMaxLength(20)
        self.municipalityLe.setMaxLength(20)
        self.departmentLe.setValidator(self.validator2)
        self.municipalityLe.setValidator(self.validator2)

        self.commentsLe = QLineEdit()

        # comb-boxes
        self.typeCb = QComboBox()
        self.latlonDatumCb = QComboBox()
        self.latlonVDatumCb = QComboBox()
        self.localProjDatumCb = QComboBox()

        # buttons
        self.addSiteButton = QPushButton('Add site')
        self.addSiteMultipleButton = QPushButton('Import sites...')
        self.closeButton = QPushButton('Close')
        self.addSiteButton.clicked.connect(self.createSingleSite)
        self.addSiteMultipleButton.clicked.connect(self.createMultipleSites)
        self.closeButton.clicked.connect(self.closeDialog)

        # set layout
        self.Grid.addWidget(self.codeLb, 0, 0)
        self.Grid.addWidget(self.nameLb, 1, 0)
        self.Grid.addWidget(self.siteTypeLb, 2, 0)
        self.Grid.addWidget(self.latitudeLb, 3, 0)
        self.Grid.addWidget(self.longitudeLb, 4, 0)
        self.Grid.addWidget(self.latlonDatumLb, 5, 0)
        self.Grid.addWidget(self.latlonVDatumLb, 6, 0)

        self.Grid.addWidget(self.codeLe, 0, 1)
        self.Grid.addWidget(self.nameLe, 1, 1)
        self.Grid.addWidget(self.typeCb, 2, 1)
        self.Grid.addWidget(self.latitudeLe, 3, 1)
        self.Grid.addWidget(self.longitudeLe, 4, 1)
        self.Grid.addWidget(self.latlonDatumCb, 5, 1)
        self.Grid.addWidget(self.latlonVDatumCb, 6, 1)

        self.Grid.addWidget(self.localxLb, 0, 2)
        self.Grid.addWidget(self.localyLb, 1, 2)
        self.Grid.addWidget(self.localProjLb, 2, 2)
        self.Grid.addWidget(self.posAccLb, 3, 2)
        self.Grid.addWidget(self.departmentLb, 4, 2)
        self.Grid.addWidget(self.municipalityLb, 5, 2)
        self.Grid.addWidget(self.commentsLb, 6, 2)

        self.Grid.addWidget(self.localxLe, 0, 3)
        self.Grid.addWidget(self.localyLe, 1, 3)
        self.Grid.addWidget(self.localProjDatumCb, 2, 3)
        self.Grid.addWidget(self.posAccLe, 3, 3)
        self.Grid.addWidget(self.departmentLe, 4, 3)
        self.Grid.addWidget(self.municipalityLe, 5, 3)
        self.Grid.addWidget(self.commentsLe, 6, 3)

        self.HLayout.addWidget(self.addSiteButton)
        self.HLayout.addWidget(self.addSiteMultipleButton)
        self.HLayout.addWidget(self.closeButton)

        self.VLayout.addLayout(self.Grid)
        self.VLayout.addLayout(self.HLayout)

        self.setLayout(self.VLayout)
        self.setMinimumWidth(500)
        self.setWindowTitle('Create new site')

        # get site CV data
        self.siteTypeDictionary = SqlQuery.get_sitetype_table(engine)
        self.srsDictionary = SqlQuery.get_srs_table(engine)
        self.vdatumDictionary = SqlQuery.get_vdatum_table(engine)

        # add data to form
        siteTypes = self.siteTypeDictionary['Term']
        srsNames = self.srsDictionary['SRSName']
        vdatum = self.vdatumDictionary['Term']

        for i in range(len(siteTypes)):
            self.typeCb.addItem(siteTypes[i])
        for i in range(len(srsNames)):
            self.latlonDatumCb.addItem(srsNames[i])
            self.localProjDatumCb.addItem(srsNames[i])
        for i in range(len(vdatum)):
            self.latlonVDatumCb.addItem(vdatum[i])

        # call dialog
        self.exec_()

    def closeDialog(self):
        self.close()

    def createSingleSite(self):
        session = startDBSession(self.engine)

        siteId = int(self.codeLe.text())
        siteCode = self.codeLe.text()
        siteName = self.nameLe.text()
        siteLatitude = np.float(self.latitudeLe.text())
        siteLongitude = np.float(self.longitudeLe.text())
        siteVerticalDatum = self.latlonVDatumCb.currentText()
        siteLocalx = self.localxLe.text()
        siteLocaly = self.localyLe.text()
        sitePossAccuracy = self.posAccLe.text()
        siteState = self.departmentLe.text()
        siteCounty = self.municipalityLe.text()
        siteType = self.typeCb.currentText()
        siteComments = self.commentsLe.text()

        srs = SqlQuery.get_srs_table(self.engine)
        srsIndex = srs['SRSName'].index(self.latlonDatumCb.currentText())
        srsLocalIndex = srs['SRSName'].index(self.localProjDatumCb.currentText())
        srsId = srs['FID'][srsIndex]
        srsLocalId = srs['FID'][srsLocalIndex]
        siteLatLongDatumId = np.int(srsId)
        siteLocalProjectionId = np.int(srsLocalId)

        if siteLocalx == '':
            siteLocalx = None
        if siteLocaly == '':
            siteLocaly = None
        if sitePossAccuracy == '':
            sitePossAccuracy = None
        if siteState == '':
            siteState = None
        if siteCounty == '':
            siteCounty = None

        # add site
        try:
            site = Sites(SiteId=siteId,
                         SiteCode=siteCode,
                         SiteName=siteName,
                         Latitude=siteLatitude,
                         Longitude=siteLongitude,
                         LatLongDatumId=siteLatLongDatumId,
                         VerticalDatum=siteVerticalDatum,
                         Localx=siteLocalx,
                         Localy=siteLocaly,
                         LocalProjectionId=siteLocalProjectionId,
                         PosAccuracy_m=sitePossAccuracy,
                         State=siteState,
                         County=siteCounty,
                         Comments=siteComments,
                         SiteType=siteType)
            session.add(site)
            session.commit()
            QMessageBox.information(self, 'Database edition', 'Site created!',
                                    QMessageBox.Ok)
        except DataError:
            session.rollback()
            QMessageBox.critical(self, 'Database edition error [DataError]', "Site couldn't be added, "
                                 "make sure provided data fits Controlled Vocabulary (CV) or "
                                 "input types", QMessageBox.Ok)
        except IntegrityError:
            session.rollback()
            QMessageBox.critical(self, 'Database edition error [IntegrityError]', "Site couldn't be added, "
                                 "make sure provided data fits Controlled Vocabulary (CV) or "
                                 "input types", QMessageBox.Ok)

    def createMultipleSites(self):
        # read .csv or excel file with sites data
        fileName, _ = CsvFileDialog.getOpenFileName(self)
        if fileName:
            if fileName[-3:] == 'csv':
                sitesDF = pd.read_csv(fileName)
            else:
                sitesDF = pd.read_excel(fileName)

            # add multiple sites dialog
            AddMultipleSites(self.engine, sitesDF)


# %% Add multiple sites form
# noinspection PyUnresolvedReferences
class AddMultipleSites(QDialog):
    def __init__(self, engine, sites_df, parent=None):
        super(AddMultipleSites, self).__init__(parent)

        self.engine = engine
        self.sitesDF = sites_df
        self.columns = list(self.sitesDF)

        # main layout
        self.VLayout = QVBoxLayout()
        self.VLayout1 = QVBoxLayout()
        self.HLayout = QHBoxLayout()
        self.Grid = QGridLayout()

        # labels
        self.codeLb = QLabel('Id / Code')
        self.nameLb = QLabel('Name')
        self.siteTypeLb = QLabel('Type')
        self.latitudeLb = QLabel('Latitude')
        self.longitudeLb = QLabel('Longitude')
        self.latlonDatumLb = QLabel('Lat / Lon datum')
        self.latlonVDatumLb = QLabel('Lat / Lon vertical datum')

        self.localxLb = QLabel('Local X (Optional)')
        self.localyLb = QLabel('Local Y (Optional)')
        self.localProjLb = QLabel('Local Projection (Optional)')
        self.posAccLb = QLabel('Position Accuracy (Optional)')
        self.departmentLb = QLabel('State (Optional)')
        self.municipalityLb = QLabel('City (Optional)')
        self.commentsLb = QLabel('Comments (Optional)')

        # combo-boxes
        self.codeCb = CboxList(self.columns)
        self.nameCb = CboxList(self.columns)
        self.siteTypeCb = QComboBox()
        self.latitudeCb = CboxList(self.columns)
        self.longitudeCb = CboxList(self.columns)
        self.latlonDatumCb = QComboBox()
        self.latlonVDatumCb = QComboBox()

        self.localxCb = CboxList(self.columns, True)
        self.localyCb = CboxList(self.columns, True)
        self.localProjDatumCb = QComboBox()
        self.posAccCb = CboxList(self.columns, True)
        self.departmentCb = CboxList(self.columns, True)
        self.municipalityCb = CboxList(self.columns, True)
        self.commentsCb = CboxList(self.columns, True)

        # buttons
        self.importSitesBttn = QPushButton('Import sites')
        self.closeBttn = QPushButton('Close')
        self.importSitesBttn.clicked.connect(self.importMultipleSites)
        self.closeBttn.clicked.connect(self.closeDialog)

        # set layout
        self.Grid.addWidget(self.codeLb, 0, 0)
        self.Grid.addWidget(self.nameLb, 1, 0)
        self.Grid.addWidget(self.siteTypeLb, 2, 0)
        self.Grid.addWidget(self.latitudeLb, 3, 0)
        self.Grid.addWidget(self.longitudeLb, 4, 0)
        self.Grid.addWidget(self.latlonDatumLb, 5, 0)
        self.Grid.addWidget(self.latlonVDatumLb, 6, 0)

        self.Grid.addWidget(self.codeCb, 0, 1)
        self.Grid.addWidget(self.nameCb, 1, 1)
        self.Grid.addWidget(self.siteTypeCb, 2, 1)
        self.Grid.addWidget(self.latitudeCb, 3, 1)
        self.Grid.addWidget(self.longitudeCb, 4, 1)
        self.Grid.addWidget(self.latlonDatumCb, 5, 1)
        self.Grid.addWidget(self.latlonVDatumCb, 6, 1)

        self.Grid.addWidget(self.localxLb, 0, 2)
        self.Grid.addWidget(self.localyLb, 1, 2)
        self.Grid.addWidget(self.localProjLb, 2, 2)
        self.Grid.addWidget(self.posAccLb, 3, 2)
        self.Grid.addWidget(self.departmentLb, 4, 2)
        self.Grid.addWidget(self.municipalityLb, 5, 2)
        self.Grid.addWidget(self.commentsLb, 6, 2)

        self.Grid.addWidget(self.localxCb, 0, 3)
        self.Grid.addWidget(self.localyCb, 1, 3)
        self.Grid.addWidget(self.localProjDatumCb, 2, 3)
        self.Grid.addWidget(self.posAccCb, 3, 3)
        self.Grid.addWidget(self.departmentCb, 4, 3)
        self.Grid.addWidget(self.municipalityCb, 5, 3)
        self.Grid.addWidget(self.commentsCb, 6, 3)

        self.HLayout.addWidget(self.importSitesBttn)
        self.HLayout.addWidget(self.closeBttn)

        self.VLayout.addLayout(self.Grid)
        self.VLayout.addLayout(self.HLayout)

        self.setLayout(self.VLayout)
        self.setMinimumWidth(500)
        self.setWindowTitle('Create multiple new sites')

        # get site CV data
        self.siteTypeDictionary = SqlQuery.get_sitetype_table(engine)
        self.srsDictionary = SqlQuery.get_srs_table(engine)
        self.vdatumDictionary = SqlQuery.get_vdatum_table(engine)

        # add data to form
        siteTypes = self.siteTypeDictionary['Term']
        srsNames = self.srsDictionary['SRSName']
        vdatum = self.vdatumDictionary['Term']

        for i in range(len(siteTypes)):
            self.siteTypeCb.addItem(siteTypes[i])
        for i in range(len(srsNames)):
            self.latlonDatumCb.addItem(srsNames[i])
            self.localProjDatumCb.addItem(srsNames[i])
        for i in range(len(vdatum)):
            self.latlonVDatumCb.addItem(vdatum[i])

        # call dialog
        self.exec_()

    # close
    def closeDialog(self):
        self.close()

    # import sites data
    def importMultipleSites(self):
        session = startDBSession(self.engine)
        register_adapter(np.int64, adapt_numpy_int64)

        # assign variables to data-frame columns
        siteCodeCol = self.codeCb.currentText()
        siteNameCol = self.nameCb.currentText()
        siteLatitudeCol = self.latitudeCb.currentText()
        siteLongitudeCol = self.longitudeCb.currentText()
        siteLocalxCol = self.localxCb.currentText()
        siteLocalyCol = self.localyCb.currentText()
        sitePossAccuracyCol = self.posAccCb.currentText()
        siteStateCol = self.departmentCb.currentText()
        siteCountyCol = self.municipalityCb.currentText()
        siteCommentsCol = self.commentsCb.currentText()

        srs = SqlQuery.get_srs_table(self.engine)
        srsIndex = srs['SRSName'].index(self.latlonDatumCb.currentText())
        srsLocalIndex = srs['SRSName'].index(self.localProjDatumCb.currentText())
        srsId = srs['FID'][srsIndex]
        srsLocalId = srs['FID'][srsLocalIndex]
        siteLatLongDatumId = np.int(srsId)
        siteLocalProjectionId = np.int(srsLocalId)
        siteVerticalDatum = self.latlonVDatumCb.currentText()
        siteType = self.siteTypeCb.currentText()

        # add each site
        try:
            for i in self.sitesDF.index:
                siteId = np.int(self.sitesDF[siteCodeCol][i])
                siteCode = self.sitesDF[siteCodeCol][i]
                siteName = self.sitesDF[siteNameCol][i]
                siteLatitude = float(self.sitesDF[siteLatitudeCol][i])
                siteLongitude = float(self.sitesDF[siteLongitudeCol][i])

                if siteLocalxCol == '<none>':
                    siteLocalx = None
                else:
                    siteLocalx = float(self.sitesDF[siteLocalxCol][i])

                if siteLocalyCol == '<none>':
                    siteLocaly = None
                else:
                    siteLocaly = float(self.sitesDF[siteLocalyCol][i])

                if sitePossAccuracyCol == '<none>':
                    sitePossAccuracy = None
                else:
                    sitePossAccuracy = float(self.sitesDF[sitePossAccuracyCol][i])

                if siteStateCol == '<none>':
                    siteState = None
                else:
                    siteState = self.sitesDF[siteStateCol][i]

                if siteCountyCol == '<none>':
                    siteCounty = None
                else:
                    siteCounty = self.sitesDF[siteCountyCol][i]

                if siteCommentsCol == '<none>':
                    siteComments = None
                else:
                    siteComments = self.sitesDF[siteCommentsCol][i]

                site = Sites(SiteId=siteId,
                             SiteCode=siteCode,
                             SiteName=siteName,
                             Latitude=siteLatitude,
                             Longitude=siteLongitude,
                             LatLongDatumId=siteLatLongDatumId,
                             VerticalDatum=siteVerticalDatum,
                             Localx=siteLocalx,
                             Localy=siteLocaly,
                             LocalProjectionId=siteLocalProjectionId,
                             PosAccuracy_m=sitePossAccuracy,
                             State=siteState,
                             County=siteCounty,
                             Comments=siteComments,
                             SiteType=siteType)

                session.add(site)

            session.commit()
            QMessageBox.information(self, 'Database edition', 'Multiple sites created!',
                                    QMessageBox.Ok)
        except DataError:
            session.rollback()
            QMessageBox.critical(self, 'Database edition error [DataError]', "Site couldn't be added, "
                                 "make sure provided data fits Controlled Vocabulary (CV) or "
                                 "input types", QMessageBox.Ok)
        except IntegrityError:
            session.rollback()
            QMessageBox.critical(self, 'Database edition error [IntegrityError]', "Site couldn't be added, "
                                 "make sure provided data fits Controlled Vocabulary (CV) or "
                                 "input types", QMessageBox.Ok)


# %% Add new source form
# noinspection PyUnresolvedReferences
class AddSource(QDialog):
    def __init__(self, engine, parent=None):
        super(AddSource, self).__init__(parent)

        self.engine = engine

        # main layout
        self.VLayout = QVBoxLayout()
        self.VLayout1 = QVBoxLayout()
        self.HLayout = QHBoxLayout()
        self.Grid = QGridLayout()

        # labels
        self.metadataLb = QLabel('Metadata ID')
        self.organizationLb = QLabel('Organization')
        self.descriptionLb = QLabel('Description')
        self.linkLb = QLabel('Link')
        self.contactNameLb = QLabel('Contact name')
        self.phoneLb = QLabel('Contact phone')

        self.emailLb = QLabel('Contact mail')
        self.addressLb = QLabel('Contact address')
        self.countyLb = QLabel('State')
        self.cityLb = QLabel('City')
        self.zipCodeLb = QLabel('Zip code')
        self.citationLb = QLabel('Citation')

        # line edits
        self.organizationLe = QLineEdit()
        self.descriptionLe = QLineEdit()
        self.linkLe = QLineEdit()
        self.contactNameLe = QLineEdit()
        self.phoneLe = QLineEdit()

        self.emailLe = QLineEdit()
        self.addressLe = QLineEdit()
        self.countyLe = QLineEdit()
        self.cityLe = QLineEdit()
        self.zipCodeLe = QLineEdit()
        self.citationLe = QLineEdit()

        # comb-boxes
        self.metadataCb = QComboBox()

        # buttons
        self.addSourceButton = QPushButton('Add source')
        self.addSourceMultipleButton = QPushButton('Import sources...')
        self.closeButton = QPushButton('Close')
        self.addSourceButton.clicked.connect(self.createSingleSource)
        self.addSourceMultipleButton.clicked.connect(self.createMultipleSources)
        self.closeButton.clicked.connect(self.closeDialog)

        # set layout
        self.Grid.addWidget(self.metadataLb, 0, 0)
        self.Grid.addWidget(self.organizationLb, 1, 0)
        self.Grid.addWidget(self.descriptionLb, 2, 0)
        self.Grid.addWidget(self.linkLb, 3, 0)
        self.Grid.addWidget(self.contactNameLb, 4, 0)
        self.Grid.addWidget(self.phoneLb, 5, 0)

        self.Grid.addWidget(self.metadataCb, 0, 1)
        self.Grid.addWidget(self.organizationLe, 1, 1)
        self.Grid.addWidget(self.descriptionLe, 2, 1)
        self.Grid.addWidget(self.linkLe, 3, 1)
        self.Grid.addWidget(self.contactNameLe, 4, 1)
        self.Grid.addWidget(self.phoneLe, 5, 1)

        self.Grid.addWidget(self.emailLb, 0, 2)
        self.Grid.addWidget(self.addressLb, 1, 2)
        self.Grid.addWidget(self.countyLb, 2, 2)
        self.Grid.addWidget(self.cityLb, 3, 2)
        self.Grid.addWidget(self.zipCodeLb, 4, 2)
        self.Grid.addWidget(self.citationLb, 5, 2)

        self.Grid.addWidget(self.emailLe, 0, 3)
        self.Grid.addWidget(self.addressLe, 1, 3)
        self.Grid.addWidget(self.countyLe, 2, 3)
        self.Grid.addWidget(self.cityLe, 3, 3)
        self.Grid.addWidget(self.zipCodeLe, 4, 3)
        self.Grid.addWidget(self.citationLe, 5, 3)

        self.HLayout.addWidget(self.addSourceButton)
        self.HLayout.addWidget(self.addSourceMultipleButton)
        self.HLayout.addWidget(self.closeButton)

        self.VLayout.addLayout(self.Grid)
        self.VLayout.addLayout(self.HLayout)

        self.setLayout(self.VLayout)
        self.setMinimumWidth(500)
        self.setWindowTitle('Create new source')

        # get metadata ID
        self.metadataIdDictionary = SqlQuery.get_metadata_table(engine)

        # add data to form
        metadataIds = self.metadataIdDictionary['ID']
        for i in range(len(metadataIds)):
            self.metadataCb.addItem(str(metadataIds[i]))

        # call dialog
        self.exec_()

    def closeDialog(self):
        self.close()

    def createSingleSource(self):
        session = startDBSession(self.engine)

        if len(SqlQuery.get_sources_table(self.engine)['ID']) != 0:
            sourceId = int(SqlQuery.get_sources_table(self.engine)['ID'][-1] + 1)
        else:
            sourceId = 1
        organization = self.organizationLe.text()
        description = self.descriptionLe.text()
        link = self.linkLe.text()
        name = self.contactNameLe.text()
        phone = self.phoneLe.text()
        email = self.emailLe.text()
        address = self.addressLe.text()
        city = self.cityLe.text()
        state = self.countyLe.text()
        zipcode = self.zipCodeLe.text()
        citation = self.citationLe.text()
        metadataId = self.metadataCb.currentText()

        # add source
        try:
            source = Sources(SourceId=sourceId,
                             Organization=organization,
                             SourceDescription=description,
                             SourceLink=link,
                             ContactName=name,
                             Phone=phone,
                             Email=email,
                             Address=address,
                             City=city,
                             State=state,
                             ZipCode=zipcode,
                             Citation=citation,
                             MetadataId=metadataId)
            session.add(source)
            session.commit()
            QMessageBox.information(self, 'Database edition', 'Source created!',
                                    QMessageBox.Ok)
        except DataError:
            session.rollback()
            QMessageBox.critical(self, 'Database edition error [DataError]', "Source couldn't be added, "
                                 "make sure provided data fits Controlled Vocabulary (CV) or "
                                 "input types", QMessageBox.Ok)
        except IntegrityError:
            session.rollback()
            QMessageBox.critical(self, 'Database edition error [IntegrityError]', "Source couldn't be added, "
                                 "make sure provided data fits Controlled Vocabulary (CV) or "
                                 "input types", QMessageBox.Ok)

    def createMultipleSources(self):
        # read .csv or excel file with sites data
        fileName, _ = CsvFileDialog.getOpenFileName(self)
        if fileName:
            if fileName[-3:] == 'csv':
                sourcesDF = pd.read_csv(fileName)
            else:
                sourcesDF = pd.read_excel(fileName)

            # add multiple sites dialog
            AddMultipleSources(self.engine, sourcesDF)


# %% Add multiple sources form
# noinspection PyUnresolvedReferences
class AddMultipleSources(QDialog):
    def __init__(self, engine, sources_df, parent=None):
        super(AddMultipleSources, self).__init__(parent)

        self.engine = engine
        self.sourcesDF = sources_df
        self.columns = list(self.sourcesDF)

        # main layout
        self.VLayout = QVBoxLayout()
        self.VLayout1 = QVBoxLayout()
        self.HLayout = QHBoxLayout()
        self.Grid = QGridLayout()

        # labels
        self.metadataLb = QLabel('Metadata ID')
        self.organizationLb = QLabel('Organization')
        self.descriptionLb = QLabel('Description')
        self.linkLb = QLabel('Link')
        self.contactNameLb = QLabel('Contact name')
        self.phoneLb = QLabel('Contact phone')

        self.emailLb = QLabel('Contact mail')
        self.addressLb = QLabel('Contact address')
        self.countyLb = QLabel('State')
        self.cityLb = QLabel('City')
        self.zipCodeLb = QLabel('Zip code')
        self.citationLb = QLabel('Citation')

        # combo-boxes
        self.metadataCb = CboxList(self.columns)
        self.organizationCb = CboxList(self.columns)
        self.descriptionCb = CboxList(self.columns)
        self.linkCb = CboxList(self.columns)
        self.contactNameCb = CboxList(self.columns)
        self.phoneCb = CboxList(self.columns)

        self.emailCb = CboxList(self.columns)
        self.addressCb = CboxList(self.columns)
        self.countyCb = CboxList(self.columns)
        self.cityCb = CboxList(self.columns)
        self.zipCodeCb = CboxList(self.columns)
        self.citationCb = CboxList(self.columns)

        # buttons
        self.importSourcesBttn = QPushButton('Import sources')
        self.closeBttn = QPushButton('Close')
        self.importSourcesBttn.clicked.connect(self.importMultipleSources)
        self.closeBttn.clicked.connect(self.closeDialog)

        # set layout
        self.Grid.addWidget(self.metadataLb, 0, 0)
        self.Grid.addWidget(self.organizationLb, 1, 0)
        self.Grid.addWidget(self.descriptionLb, 2, 0)
        self.Grid.addWidget(self.linkLb, 3, 0)
        self.Grid.addWidget(self.contactNameLb, 4, 0)
        self.Grid.addWidget(self.phoneLb, 5, 0)

        self.Grid.addWidget(self.metadataCb, 0, 1)
        self.Grid.addWidget(self.organizationCb, 1, 1)
        self.Grid.addWidget(self.descriptionCb, 2, 1)
        self.Grid.addWidget(self.linkCb, 3, 1)
        self.Grid.addWidget(self.contactNameCb, 4, 1)
        self.Grid.addWidget(self.phoneCb, 5, 1)

        self.Grid.addWidget(self.emailLb, 0, 2)
        self.Grid.addWidget(self.addressLb, 1, 2)
        self.Grid.addWidget(self.countyLb, 2, 2)
        self.Grid.addWidget(self.cityLb, 3, 2)
        self.Grid.addWidget(self.zipCodeLb, 4, 2)
        self.Grid.addWidget(self.citationLb, 5, 2)

        self.Grid.addWidget(self.emailCb, 0, 3)
        self.Grid.addWidget(self.addressCb, 1, 3)
        self.Grid.addWidget(self.countyCb, 2, 3)
        self.Grid.addWidget(self.cityCb, 3, 3)
        self.Grid.addWidget(self.zipCodeCb, 4, 3)
        self.Grid.addWidget(self.citationCb, 5, 3)

        self.HLayout.addWidget(self.importSourcesBttn)
        self.HLayout.addWidget(self.closeBttn)

        self.VLayout.addLayout(self.Grid)
        self.VLayout.addLayout(self.HLayout)

        self.setLayout(self.VLayout)
        self.setMinimumWidth(500)
        self.setWindowTitle('Create multiple new sources')

        # call dialog
        self.exec_()

    # close
    def closeDialog(self):
        self.close()

    # import sites data
    def importMultipleSources(self):
        session = startDBSession(self.engine)
        register_adapter(np.int64, adapt_numpy_int64)

        if len(SqlQuery.get_sources_table(self.engine)['ID']) != 0:
            sourceId = int(SqlQuery.get_sources_table(self.engine)['ID'][-1] + 1)
        else:
            sourceId = 1

        # assign variables to data-frame columns
        metadataCol = self.metadataCb.currentText()
        orgCol = self.organizationCb.currentText()
        descCol = self.descriptionCb.currentText()
        linkCol = self.linkCb.currentText()
        contactCol = self.contactNameCb.currentText()
        phoneCol = self.phoneCb.currentText()
        emailCol = self.emailCb.currentText()
        addressCol = self.addressCb.currentText()
        stateCol = self.countyCb.currentText()
        cityCol = self.cityCb.currentText()
        zipCol = self.zipCodeCb.currentText()
        citationCol = self.citationCb.currentText()

        # add each source
        try:
            for i in self.sourcesDF.index:
                metadata = self.sourcesDF[metadataCol][i]
                organization = self.sourcesDF[orgCol][i]
                description = self.sourcesDF[descCol][i]
                link = self.sourcesDF[linkCol][i]
                contact = self.sourcesDF[contactCol][i]
                phone = self.sourcesDF[phoneCol][i]
                email = self.sourcesDF[emailCol][i]
                address = self.sourcesDF[addressCol][i]
                state = self.sourcesDF[stateCol][i]
                city = self.sourcesDF[cityCol][i]
                zipCode = self.sourcesDF[zipCol][i]
                citation = self.sourcesDF[citationCol][i]

                source = Sources(SourceId=sourceId,
                                 Organization=organization,
                                 SourceDescription=description,
                                 SourceLink=link,
                                 ContactName=contact,
                                 Phone=phone,
                                 Email=email,
                                 Address=address,
                                 City=city,
                                 State=state,
                                 ZipCode=zipCode,
                                 Citation=citation,
                                 MetadataId=metadata)
                session.add(source)
                sourceId += 1

            session.commit()
            QMessageBox.information(self, 'Database edition', 'Multiple sources created!',
                                    QMessageBox.Ok)
        except DataError:
            session.rollback()
            QMessageBox.critical(self, 'Database edition error [DataError]', "Source couldn't be added, "
                                 "make sure provided data fits Controlled Vocabulary (CV) or "
                                 "input types", QMessageBox.Ok)
        except IntegrityError:
            session.rollback()
            QMessageBox.critical(self, 'Database edition error [IntegrityError]', "Source couldn't be added, "
                                 "make sure provided data fits Controlled Vocabulary (CV) or "
                                 "input types", QMessageBox.Ok)


# %% Add new variable [form]
# noinspection PyUnresolvedReferences
class AddVariable(QDialog):
    def __init__(self, engine, parent=None):
        super(AddVariable, self).__init__(parent)

        self.engine = engine

        # main layout
        self.VLayout = QVBoxLayout()
        self.VLayout1 = QVBoxLayout()
        self.HLayout = QHBoxLayout()
        self.Grid = QGridLayout()

        # labels
        self.codeLb = QLabel('Code')
        self.nameLb = QLabel('Name')
        self.speciationLb = QLabel('Speciation')
        self.vUnitsLb = QLabel('Units')
        self.mediumLb = QLabel('Sample medium')
        self.vTypeLb = QLabel('Value type')

        self.isRegularLb = QLabel('Is regular')
        self.tSupportLb = QLabel('Time span')
        self.tUnitsLb = QLabel('Time units')
        self.dTypeLb = QLabel('Data type')
        self.categoryLb = QLabel('Category')
        self.noDataLb = QLabel('No data value')

        # line edits
        self.codeLe = QLineEdit()
        self.codeLe.setMaxLength(8)
        self.regex1 = QRegExp("[0-9]+")
        self.validator1 = QRegExpValidator(self.regex1)
        self.codeLe.setValidator(self.validator1)

        self.noDataLe = QLineEdit()
        self.noDataLe.setMaxLength(8)
        self.regex3 = QRegExp("[-+]?([0-9]+)")
        self.validator3 = QRegExpValidator(self.regex3)
        self.noDataLe.setValidator(self.validator3)

        self.tSupportLe = QLineEdit()
        self.tSupportLe.setMaxLength(2)
        self.tSupportLe.setValidator(self.validator1)

        # comb-boxes
        self.nameCb = CboxList(SqlQuery.get_varName_table(engine)['Term'])
        self.speciationCb = CboxList(SqlQuery.get_speciation_table(engine)['Term'])
        self.vUnitsCb = CboxList(SqlQuery.get_units_table(engine)['Name'])
        self.mediumCb = CboxList(SqlQuery.get_sampleMedium_table(engine)['Term'])
        self.vTypeCb = CboxList(SqlQuery.get_valueType_table(engine)['Term'])

        self.isRegularCb = CboxList(['True', 'False'])
        self.tUnitsCb = CboxList(SqlQuery.get_units_table(engine)['Name'])
        self.dTypeCb = CboxList(SqlQuery.get_dataType_table(engine)['Term'])
        self.categoryCb = CboxList(SqlQuery.get_category_table(engine)['Term'])

        # buttons
        self.addVariableButton = QPushButton('Add variable')
        self.addVariableMultipleButton = QPushButton('Import variables...')
        self.closeButton = QPushButton('Close')
        self.addVariableButton.clicked.connect(self.createSingleVariable)
        self.addVariableMultipleButton.clicked.connect(self.createMultipleVariables)
        self.closeButton.clicked.connect(self.closeDialog)

        # set layout
        self.Grid.addWidget(self.codeLb, 0, 0)
        self.Grid.addWidget(self.nameLb, 1, 0)
        self.Grid.addWidget(self.speciationLb, 2, 0)
        self.Grid.addWidget(self.vUnitsLb, 3, 0)
        self.Grid.addWidget(self.mediumLb, 4, 0)
        self.Grid.addWidget(self.vTypeLb, 5, 0)

        self.Grid.addWidget(self.codeLe, 0, 1)
        self.Grid.addWidget(self.nameCb, 1, 1)
        self.Grid.addWidget(self.speciationCb, 2, 1)
        self.Grid.addWidget(self.vUnitsCb, 3, 1)
        self.Grid.addWidget(self.mediumCb, 4, 1)
        self.Grid.addWidget(self.vTypeCb, 5, 1)

        self.Grid.addWidget(self.isRegularLb, 0, 2)
        self.Grid.addWidget(self.tSupportLb, 1, 2)
        self.Grid.addWidget(self.tUnitsLb, 2, 2)
        self.Grid.addWidget(self.dTypeLb, 3, 2)
        self.Grid.addWidget(self.categoryLb, 4, 2)
        self.Grid.addWidget(self.noDataLb, 5, 2)

        self.Grid.addWidget(self.isRegularCb, 0, 3)
        self.Grid.addWidget(self.tSupportLe, 1, 3)
        self.Grid.addWidget(self.tUnitsCb, 2, 3)
        self.Grid.addWidget(self.dTypeCb, 3, 3)
        self.Grid.addWidget(self.categoryCb, 4, 3)
        self.Grid.addWidget(self.noDataLe, 5, 3)

        self.HLayout.addWidget(self.addVariableButton)
        self.HLayout.addWidget(self.addVariableMultipleButton)
        self.HLayout.addWidget(self.closeButton)

        self.VLayout.addLayout(self.Grid)
        self.VLayout.addLayout(self.HLayout)

        self.setLayout(self.VLayout)
        self.setMinimumWidth(500)
        self.setWindowTitle('Create new variable')

        # call dialog
        self.exec_()

    def closeDialog(self):
        self.close()

    def createSingleVariable(self):
        session = startDBSession(self.engine)

        code = self.codeLe.text()
        varId = int(self.codeLe.text())
        name = self.nameCb.currentText()
        speciation = self.speciationCb.currentText()
        sampleMedium = self.mediumCb.currentText()
        vType = self.vTypeCb.currentText()
        isRegular = self.isRegularCb.currentText()
        tSupport = self.tSupportLe.text()
        dType = self.dTypeCb.currentText()
        category = self.categoryCb.currentText()
        noData = self.noDataLe.text()

        units = SqlQuery.get_units_table(self.engine)
        unitsIndex = units['Name'].index(self.vUnitsCb.currentText())
        tUnitsIndex = units['Name'].index(self.tUnitsCb.currentText())
        unitsId = units['ID'][unitsIndex]
        tUnitsId = units['ID'][tUnitsIndex]
        vUnitsId = np.int(unitsId)

        if isRegular == 'True':
            isRegular = True
        else:
            isRegular = False

        # add variable
        try:
            variable = Variables(VariableId=varId,
                                 VariableCode=code,
                                 VariableName=name,
                                 Speciation=speciation,
                                 VariableUnitsId=vUnitsId,
                                 SampleMedium=sampleMedium,
                                 ValueType=vType,
                                 IsRegular=isRegular,
                                 TimeSupport=tSupport,
                                 TimeUnitsId=tUnitsId,
                                 DataType=dType,
                                 GeneralCategory=category,
                                 NoDataValue=noData)

            session.add(variable)
            session.commit()
            QMessageBox.information(self, 'Database edition', 'Variable created!',
                                    QMessageBox.Ok)
        except DataError:
            session.rollback()
            QMessageBox.critical(self, 'Database edition error [DataError]', "Variable couldn't be added, "
                                 "make sure provided data fits Controlled Vocabulary (CV) or "
                                 "input types", QMessageBox.Ok)
        except IntegrityError:
            session.rollback()
            QMessageBox.critical(self, 'Database edition error [IntegrityError]', "Variable couldn't be added, "
                                 "make sure provided data fits Controlled Vocabulary (CV) or "
                                 "input types", QMessageBox.Ok)

    def createMultipleVariables(self):
        # read .csv or excel file with sites data
        fileName, _ = CsvFileDialog.getOpenFileName(self)
        if fileName:
            if fileName[-3:] == 'csv':
                variablesDF = pd.read_csv(fileName)
            else:
                variablesDF = pd.read_excel(fileName)

            # add multiple sites dialog
            AddMultipleVariables(self.engine, variablesDF)


# %% Add multiple variables form
# noinspection PyUnresolvedReferences
class AddMultipleVariables(QDialog):
    def __init__(self, engine, variables_df, parent=None):
        super(AddMultipleVariables, self).__init__(parent)

        self.engine = engine
        self.variablesDF = variables_df
        self.columns = list(self.variablesDF)

        # main layout
        self.VLayout = QVBoxLayout()
        self.VLayout1 = QVBoxLayout()
        self.HLayout = QHBoxLayout()
        self.Grid = QGridLayout()

        # labels
        self.codeLb = QLabel('Code')
        self.nameLb = QLabel('Name')
        self.speciationLb = QLabel('Speciation')
        self.vUnitsLb = QLabel('Units')
        self.mediumLb = QLabel('Sample medium')
        self.vTypeLb = QLabel('Value type')

        self.isRegularLb = QLabel('Is regular')
        self.tSupportLb = QLabel('Time support')
        self.tUnitsLb = QLabel('Time units')
        self.dTypeLb = QLabel('Data type')
        self.categoryLb = QLabel('Category')
        self.noDataLb = QLabel('No data value')

        # combo-boxes
        self.codeCb = CboxList(self.columns)
        self.nameCb = CboxList(self.columns)
        self.speciationCb = CboxList(self.columns)
        self.vUnitsCb = CboxList(self.columns)
        self.mediumCb = CboxList(self.columns)
        self.vTypeCb = CboxList(self.columns)

        self.isRegularCb = CboxList(self.columns)
        self.tSupportCb = CboxList(self.columns)
        self.tUnitsCb = CboxList(self.columns)
        self.dTypeCb = CboxList(self.columns)
        self.categoryCb = CboxList(self.columns)
        self.noDataCb = CboxList(self.columns)

        # buttons
        self.importVariablesBttn = QPushButton('Import variables')
        self.closeBttn = QPushButton('Close')
        self.importVariablesBttn.clicked.connect(self.importMultipleVariables)
        self.closeBttn.clicked.connect(self.closeDialog)

        # set layout
        self.Grid.addWidget(self.codeLb, 0, 0)
        self.Grid.addWidget(self.nameLb, 1, 0)
        self.Grid.addWidget(self.speciationLb, 2, 0)
        self.Grid.addWidget(self.vUnitsLb, 3, 0)
        self.Grid.addWidget(self.mediumLb, 4, 0)
        self.Grid.addWidget(self.vTypeLb, 5, 0)

        self.Grid.addWidget(self.codeCb, 0, 1)
        self.Grid.addWidget(self.nameCb, 1, 1)
        self.Grid.addWidget(self.speciationCb, 2, 1)
        self.Grid.addWidget(self.vUnitsCb, 3, 1)
        self.Grid.addWidget(self.mediumCb, 4, 1)
        self.Grid.addWidget(self.vTypeCb, 5, 1)

        self.Grid.addWidget(self.isRegularLb, 0, 2)
        self.Grid.addWidget(self.tSupportLb, 1, 2)
        self.Grid.addWidget(self.tUnitsLb, 2, 2)
        self.Grid.addWidget(self.dTypeLb, 3, 2)
        self.Grid.addWidget(self.categoryLb, 4, 2)
        self.Grid.addWidget(self.noDataLb, 5, 2)

        self.Grid.addWidget(self.isRegularCb, 0, 3)
        self.Grid.addWidget(self.tSupportCb, 1, 3)
        self.Grid.addWidget(self.tUnitsCb, 2, 3)
        self.Grid.addWidget(self.dTypeCb, 3, 3)
        self.Grid.addWidget(self.categoryCb, 4, 3)
        self.Grid.addWidget(self.noDataCb, 5, 3)

        self.HLayout.addWidget(self.importVariablesBttn)
        self.HLayout.addWidget(self.closeBttn)

        self.VLayout.addLayout(self.Grid)
        self.VLayout.addLayout(self.HLayout)

        self.setLayout(self.VLayout)
        self.setMinimumWidth(500)
        self.setWindowTitle('Create multiple new variables')

        # call dialog
        self.exec_()

    # close
    def closeDialog(self):
        self.close()

    # import sites data
    def importMultipleVariables(self):
        session = startDBSession(self.engine)
        register_adapter(np.int64, adapt_numpy_int64)

        # assign variables to data-frame columns
        codeCol = self.codeCb.currentText()
        nameCol = self.nameCb.currentText()
        speciationCol = self.speciationCb.currentText()
        vUnitsCol = self.vUnitsCb.currentText()
        mediumCol = self.mediumCb.currentText()
        vTypeCol = self.vTypeCb.currentText()
        isRegularCol = self.isRegularCb.currentText()
        tSupportCol = self.tSupportCb.currentText()
        tUnitsCol = self.tUnitsCb.currentText()
        dTypeCol = self.dTypeCb.currentText()
        categoryCol = self.categoryCb.currentText()
        noDataCol = self.noDataCb.currentText()

        # add each source
        try:
            for i in self.variablesDF.index:
                varId = self.variablesDF[codeCol][i]
                code = self.variablesDF[codeCol][i]
                name = self.variablesDF[nameCol][i]
                speciation = self.variablesDF[speciationCol][i]
                vUnitsId = self.variablesDF[vUnitsCol][i]
                sampleMedium = self.variablesDF[mediumCol][i]
                vType = self.variablesDF[vTypeCol][i]
                isRegular = self.variablesDF[isRegularCol][i]
                tSupport = self.variablesDF[tSupportCol][i]
                tUnitsId = self.variablesDF[tUnitsCol][i]
                dType = self.variablesDF[dTypeCol][i]
                category = self.variablesDF[categoryCol][i]
                noData = self.variablesDF[noDataCol][i]

                if isRegular == 1:
                    isRegular = True
                else:
                    isRegular = False

                variable = Variables(VariableId=varId,
                                     VariableCode=code,
                                     VariableName=name,
                                     Speciation=speciation,
                                     VariableUnitsId=vUnitsId,
                                     SampleMedium=sampleMedium,
                                     ValueType=vType,
                                     IsRegular=isRegular,
                                     TimeSupport=tSupport,
                                     TimeUnitsId=tUnitsId,
                                     DataType=dType,
                                     GeneralCategory=category,
                                     NoDataValue=noData)
                session.add(variable)

            session.commit()
            QMessageBox.information(self, 'Database edition', 'Multiple variables created!',
                                    QMessageBox.Ok)
        except DataError:
            session.rollback()
            QMessageBox.critical(self, 'Database edition error [DataError]', "Variable couldn't be added, "
                                 "make sure provided data fits Controlled Vocabulary (CV) or "
                                 "input types", QMessageBox.Ok)
        except IntegrityError:
            session.rollback()
            QMessageBox.critical(self, 'Database edition error [IntegrityError]', "Variable couldn't be added, "
                                 "make sure provided data fits Controlled Vocabulary (CV) or "
                                 "input types", QMessageBox.Ok)


# %% Add new qualifier [form]
# noinspection PyUnresolvedReferences
class AddQualifier(QDialog):
    def __init__(self, engine, parent=None):
        super(AddQualifier, self).__init__(parent)

        self.engine = engine

        # main layout
        self.VLayout = QVBoxLayout()
        self.VLayout1 = QVBoxLayout()
        self.HLayout = QHBoxLayout()
        self.Grid = QGridLayout()

        # labels
        self.idLb = QLabel('ID')
        self.codeLb = QLabel('Code')
        self.descLb = QLabel('Description')

        # line edits
        self.idLe = QLineEdit()
        self.idLe.setMaxLength(8)
        self.regex1 = QRegExp("[0-9]+")
        self.validator1 = QRegExpValidator(self.regex1)
        self.idLe.setValidator(self.validator1)

        self.codeLe = QLineEdit()
        self.descLe = QLineEdit()
        self.regex2 = QRegExp("[0-9-a-z-A-Z\s]+")
        self.validator2 = QRegExpValidator(self.regex2)
        self.codeLe.setValidator(self.validator2)
        self.descLe.setValidator(self.validator2)

        # buttons
        self.addQualifierButton = QPushButton('Add qualifier')
        self.addQualifierMultipleButton = QPushButton('Import qualifiers...')
        self.closeButton = QPushButton('Close')
        self.addQualifierButton.clicked.connect(self.createSingleQualifier)
        self.addQualifierMultipleButton.clicked.connect(self.createMultipleQualifiers)
        self.closeButton.clicked.connect(self.closeDialog)

        # set layout
        self.Grid.addWidget(self.idLb, 0, 0)
        self.Grid.addWidget(self.codeLb, 1, 0)
        self.Grid.addWidget(self.descLb, 2, 0)

        self.Grid.addWidget(self.idLe, 0, 1)
        self.Grid.addWidget(self.codeLe, 1, 1)
        self.Grid.addWidget(self.descLe, 2, 1)

        self.HLayout.addWidget(self.addQualifierButton)
        self.HLayout.addWidget(self.addQualifierMultipleButton)
        self.HLayout.addWidget(self.closeButton)

        self.VLayout.addLayout(self.Grid)
        self.VLayout.addLayout(self.HLayout)

        self.setLayout(self.VLayout)
        self.setMinimumWidth(500)
        self.setWindowTitle('Create new qualifier')

        # call dialog
        self.exec_()

    def closeDialog(self):
        self.close()

    def createSingleQualifier(self):
        session = startDBSession(self.engine)

        qualId = int(self.idLe.text())
        code = self.codeLe.text()
        description = self.descLe.text()

        # add qualifier
        try:
            qualifier = Qualifiers(QualifierId=qualId,
                                   QualifierCode=code,
                                   QualifierDescription=description)

            session.add(qualifier)
            session.commit()
            QMessageBox.information(self, 'Database edition', 'Qualifier created!',
                                    QMessageBox.Ok)
        except DataError:
            session.rollback()
            QMessageBox.critical(self, 'Database edition error [DataError]', "Qualifier couldn't be added, "
                                 "make sure provided data fits Controlled Vocabulary (CV) or "
                                 "input types", QMessageBox.Ok)
        except IntegrityError:
            session.rollback()
            QMessageBox.critical(self, 'Database edition error [IntegrityError]', "Qualifier couldn't be added, "
                                 "make sure provided data fits Controlled Vocabulary (CV) or "
                                 "input types", QMessageBox.Ok)

    def createMultipleQualifiers(self):
        # read .csv or excel file with sites data
        fileName, _ = CsvFileDialog.getOpenFileName(self)
        if fileName:
            if fileName[-3:] == 'csv':
                qualifierDF = pd.read_csv(fileName)
            else:
                qualifierDF = pd.read_excel(fileName)

            # add multiple sites dialog
            AddMultipleQualifiers(self.engine, qualifierDF)


# %% Add multiple qualifiers form
# noinspection PyUnresolvedReferences
class AddMultipleQualifiers(QDialog):
    def __init__(self, engine, qual_df, parent=None):
        super(AddMultipleQualifiers, self).__init__(parent)

        self.engine = engine
        self.qualDF = qual_df
        self.columns = list(self.qualDF)

        # main layout
        self.VLayout = QVBoxLayout()
        self.VLayout1 = QVBoxLayout()
        self.HLayout = QHBoxLayout()
        self.Grid = QGridLayout()

        # labels
        self.idLb = QLabel('ID')
        self.codeLb = QLabel('Code')
        self.descLb = QLabel('Description')

        # combo-boxes
        self.idCb = CboxList(self.columns)
        self.codeCb = CboxList(self.columns)
        self.descCb = CboxList(self.columns)

        # buttons
        self.importQualsBttn = QPushButton('Import qualifiers')
        self.closeBttn = QPushButton('Close')
        self.importQualsBttn.clicked.connect(self.importMultipleQualifiers)
        self.closeBttn.clicked.connect(self.closeDialog)

        # set layout
        self.Grid.addWidget(self.idLb, 0, 0)
        self.Grid.addWidget(self.codeLb, 1, 0)
        self.Grid.addWidget(self.descLb, 2, 0)

        self.Grid.addWidget(self.idCb, 0, 1)
        self.Grid.addWidget(self.codeCb, 1, 1)
        self.Grid.addWidget(self.descCb, 2, 1)

        self.HLayout.addWidget(self.importQualsBttn)
        self.HLayout.addWidget(self.closeBttn)

        self.VLayout.addLayout(self.Grid)
        self.VLayout.addLayout(self.HLayout)

        self.setLayout(self.VLayout)
        self.setMinimumWidth(500)
        self.setWindowTitle('Create multiple new qualifiers')

        # call dialog
        self.exec_()

    # close
    def closeDialog(self):
        self.close()

    # import sites data
    def importMultipleQualifiers(self):
        session = startDBSession(self.engine)
        register_adapter(np.int64, adapt_numpy_int64)

        # assign variables to data-frame columns
        idCol = self.idCb.currentText()
        codeCol = self.codeCb.currentText()
        descCol = self.descCb.currentText()
        # add each source
        try:
            for i in self.qualDF.index:
                qualId = int(self.qualDF[idCol][i])
                code = self.qualDF[codeCol][i]
                description = self.qualDF[descCol][i]

                qualifier = Qualifiers(QualifierId=qualId,
                                       QualifierCode=code,
                                       QualifierDescription=description)
                session.add(qualifier)

            session.commit()
            QMessageBox.information(self, 'Database edition', 'Multiple qualifiers created!',
                                    QMessageBox.Ok)
        except DataError:
            session.rollback()
            QMessageBox.critical(self, 'Database edition error [DataError]', "Qualifier couldn't be added, "
                                 "make sure provided data fits Controlled Vocabulary (CV) or "
                                 "input types", QMessageBox.Ok)
        except IntegrityError:
            session.rollback()
            QMessageBox.critical(self, 'Database edition error [IntegrityError]', "Qualifier couldn't be added, "
                                 "make sure provided data fits Controlled Vocabulary (CV) or "
                                 "input types", QMessageBox.Ok)


# %% Add new method [form]
# noinspection PyUnresolvedReferences
class AddMethod(QDialog):
    def __init__(self, engine, parent=None):
        super(AddMethod, self).__init__(parent)

        self.engine = engine

        # main layout
        self.VLayout = QVBoxLayout()
        self.VLayout1 = QVBoxLayout()
        self.HLayout = QHBoxLayout()
        self.Grid = QGridLayout()

        # labels
        self.codeLb = QLabel('ID')
        self.descLb = QLabel('Description')
        self.linkLb = QLabel('Link')

        # line edits
        self.codeLe = QLineEdit()
        self.codeLe.setMaxLength(8)
        self.regex1 = QRegExp("[0-9]+")
        self.validator1 = QRegExpValidator(self.regex1)
        self.codeLe.setValidator(self.validator1)

        self.descLe = QLineEdit()
        self.regex2 = QRegExp("[0-9-a-z-A-Z\s]+")
        self.validator2 = QRegExpValidator(self.regex2)
        self.descLe.setValidator(self.validator2)

        self.linkLe = QLineEdit()

        # buttons
        self.addMethodButton = QPushButton('Add method')
        self.addMethodMultipleButton = QPushButton('Import methods...')
        self.closeButton = QPushButton('Close')
        self.addMethodButton.clicked.connect(self.createSingleMethod)
        self.addMethodMultipleButton.clicked.connect(self.createMultipleMethods)
        self.closeButton.clicked.connect(self.closeDialog)

        # set layout
        self.Grid.addWidget(self.codeLb, 0, 0)
        self.Grid.addWidget(self.descLb, 1, 0)
        self.Grid.addWidget(self.linkLb, 2, 0)

        self.Grid.addWidget(self.codeLe, 0, 1)
        self.Grid.addWidget(self.descLe, 1, 1)
        self.Grid.addWidget(self.linkLe, 2, 1)

        self.HLayout.addWidget(self.addMethodButton)
        self.HLayout.addWidget(self.addMethodMultipleButton)
        self.HLayout.addWidget(self.closeButton)

        self.VLayout.addLayout(self.Grid)
        self.VLayout.addLayout(self.HLayout)

        self.setLayout(self.VLayout)
        self.setMinimumWidth(500)
        self.setWindowTitle('Create new method')

        # call dialog
        self.exec_()

    def closeDialog(self):
        self.close()

    def createSingleMethod(self):
        session = startDBSession(self.engine)

        code = self.codeLe.text()
        description = self.descLe.text()
        link = self.linkLe.text()

        # add qualifier
        try:
            method = Methods(MethodId=code,
                             MethodDescription=description,
                             MethodLink=link)

            session.add(method)
            session.commit()
            QMessageBox.information(self, 'Database edition', 'Method created!',
                                    QMessageBox.Ok)
        except DataError:
            session.rollback()
            QMessageBox.critical(self, 'Database edition error [DataError]', "Method couldn't be added, "
                                 "make sure provided data fits Controlled Vocabulary (CV) or "
                                 "input types", QMessageBox.Ok)
        except IntegrityError:
            session.rollback()
            QMessageBox.critical(self, 'Database edition error [IntegrityError]', "Method couldn't be added, "
                                 "make sure provided data fits Controlled Vocabulary (CV) or "
                                 "input types", QMessageBox.Ok)

    def createMultipleMethods(self):
        # read .csv or excel file with sites data
        fileName, _ = CsvFileDialog.getOpenFileName(self)
        if fileName:
            if fileName[-3:] == 'csv':
                methodDF = pd.read_csv(fileName)
            else:
                methodDF = pd.read_excel(fileName)

            # add multiple sites dialog
            AddMultipleMethods(self.engine, methodDF)


# %% Add multiple methods form
# noinspection PyUnresolvedReferences
class AddMultipleMethods(QDialog):
    def __init__(self, engine, methods_df, parent=None):
        super(AddMultipleMethods, self).__init__(parent)

        self.engine = engine
        self.methodsDF = methods_df
        self.columns = list(self.methodsDF)

        # main layout
        self.VLayout = QVBoxLayout()
        self.VLayout1 = QVBoxLayout()
        self.HLayout = QHBoxLayout()
        self.Grid = QGridLayout()

        # labels
        self.codeLb = QLabel('ID')
        self.descLb = QLabel('Description')
        self.linkLb = QLabel('Link')

        # combo-boxes
        self.codeCb = CboxList(self.columns)
        self.descCb = CboxList(self.columns)
        self.linkCb = CboxList(self.columns)

        # buttons
        self.importMethodsBttn = QPushButton('Import methods')
        self.closeBttn = QPushButton('Close')
        self.importMethodsBttn.clicked.connect(self.importMultipleMethods)
        self.closeBttn.clicked.connect(self.closeDialog)

        # set layout
        self.Grid.addWidget(self.codeLb, 0, 0)
        self.Grid.addWidget(self.descLb, 1, 0)
        self.Grid.addWidget(self.linkLb, 2, 0)

        self.Grid.addWidget(self.codeCb, 0, 1)
        self.Grid.addWidget(self.descCb, 1, 1)
        self.Grid.addWidget(self.linkCb, 2, 1)

        self.HLayout.addWidget(self.importMethodsBttn)
        self.HLayout.addWidget(self.closeBttn)

        self.VLayout.addLayout(self.Grid)
        self.VLayout.addLayout(self.HLayout)

        self.setLayout(self.VLayout)
        self.setMinimumWidth(500)
        self.setWindowTitle('Create multiple new methods')

        # call dialog
        self.exec_()

    # close
    def closeDialog(self):
        self.close()

    # import sites data
    def importMultipleMethods(self):
        session = startDBSession(self.engine)
        register_adapter(np.int64, adapt_numpy_int64)

        # assign variables to data-frame columns
        codeCol = self.codeCb.currentText()
        descCol = self.descCb.currentText()
        linkCol = self.linkCb.currentText()
        # add each source
        try:
            for i in self.methodsDF.index:
                code = self.methodsDF[codeCol][i]
                description = self.methodsDF[descCol][i]
                link = self.methodsDF[linkCol][i]

                method = Methods(MethodId=code,
                                 MethodDescription=description,
                                 MethodLink=link)
                session.add(method)

            session.commit()
            QMessageBox.information(self, 'Database edition', 'Multiple methods created!',
                                    QMessageBox.Ok)
        except DataError:
            session.rollback()
            QMessageBox.critical(self, 'Database edition error [DataError]', "Method couldn't be added, "
                                 "make sure provided data fits Controlled Vocabulary (CV) or "
                                 "input types", QMessageBox.Ok)
        except IntegrityError:
            session.rollback()
            QMessageBox.critical(self, 'Database edition error [IntegrityError]', "Method couldn't be added, "
                                 "make sure provided data fits Controlled Vocabulary (CV) or "
                                 "input types", QMessageBox.Ok)


# %% Add new quality control level [form]
# noinspection PyUnresolvedReferences
class AddQuality(QDialog):
    def __init__(self, engine, parent=None):
        super(AddQuality, self).__init__(parent)

        self.engine = engine

        # main layout
        self.VLayout = QVBoxLayout()
        self.VLayout1 = QVBoxLayout()
        self.HLayout = QHBoxLayout()
        self.Grid = QGridLayout()

        # labels
        self.idLb = QLabel('ID')
        self.codeLb = QLabel('Code')
        self.defLb = QLabel('Definition')
        self.expLb = QLabel('Explanation')

        # line edits
        self.idLe = QLineEdit()
        self.idLe.setMaxLength(8)
        self.regex1 = QRegExp("[0-9]+")
        self.validator1 = QRegExpValidator(self.regex1)
        self.idLe.setValidator(self.validator1)

        self.codeLe = QLineEdit()
        self.defLe = QLineEdit()
        self.regex2 = QRegExp("[0-9-a-z-A-Z\s]+")
        self.validator2 = QRegExpValidator(self.regex2)
        self.codeLe.setValidator(self.validator2)
        self.defLe.setValidator(self.validator2)

        self.expLe = QLineEdit()

        # buttons
        self.addQualityButton = QPushButton('Add quality control level')
        self.addQualityMultipleButton = QPushButton('Import qualities...')
        self.closeButton = QPushButton('Close')
        self.addQualityButton.clicked.connect(self.createSingleQuality)
        self.addQualityMultipleButton.clicked.connect(self.createMultipleQualities)
        self.closeButton.clicked.connect(self.closeDialog)

        # set layout
        self.Grid.addWidget(self.idLb, 0, 0)
        self.Grid.addWidget(self.codeLb, 1, 0)
        self.Grid.addWidget(self.defLb, 2, 0)
        self.Grid.addWidget(self.expLb, 3, 0)

        self.Grid.addWidget(self.idLe, 0, 1)
        self.Grid.addWidget(self.codeLe, 1, 1)
        self.Grid.addWidget(self.defLe, 2, 1)
        self.Grid.addWidget(self.expLe, 3, 1)

        self.HLayout.addWidget(self.addQualityButton)
        self.HLayout.addWidget(self.addQualityMultipleButton)
        self.HLayout.addWidget(self.closeButton)

        self.VLayout.addLayout(self.Grid)
        self.VLayout.addLayout(self.HLayout)

        self.setLayout(self.VLayout)
        self.setMinimumWidth(500)
        self.setWindowTitle('Create new quality control level')

        # call dialog
        self.exec_()

    def closeDialog(self):
        self.close()

    def createSingleQuality(self):
        session = startDBSession(self.engine)

        qualId = int(self.idLe.text())
        code = self.codeLe.text()
        definition = self.defLe.text()
        explanation = self.expLe.text()

        # add qualifier
        try:
            quality = QualityControlLevels(QualityControlLevelId=qualId,
                                           QualityControlLevelCode=code,
                                           Definition=definition,
                                           Explanation=explanation)
            session.add(quality)
            session.commit()
            QMessageBox.information(self, 'Database edition', 'Quality control level created!',
                                    QMessageBox.Ok)
        except DataError:
            session.rollback()
            QMessageBox.critical(self, 'Database edition error [DataError]', "Quality control level "
                                 "couldn't be added, make sure provided data fits Controlled "
                                 "Vocabulary (CV) or input types", QMessageBox.Ok)
        except IntegrityError:
            session.rollback()
            QMessageBox.critical(self, 'Database edition error [DataError]', "Quality control level "
                                 "couldn't be added, make sure provided data fits Controlled "
                                 "Vocabulary (CV) or input types", QMessageBox.Ok)

    def createMultipleQualities(self):
        # read .csv or excel file with sites data
        fileName, _ = CsvFileDialog.getOpenFileName(self)
        if fileName:
            if fileName[-3:] == 'csv':
                qualDF = pd.read_csv(fileName)
            else:
                qualDF = pd.read_excel(fileName)

            # add multiple sites dialog
            AddMultipleQualities(self.engine, qualDF)


# %% Add multiple quality control level [form]
# noinspection PyUnresolvedReferences
class AddMultipleQualities(QDialog):
    def __init__(self, engine, qual_df, parent=None):
        super(AddMultipleQualities, self).__init__(parent)

        self.engine = engine
        self.qualDF = qual_df
        self.columns = list(self.qualDF)

        # main layout
        self.VLayout = QVBoxLayout()
        self.VLayout1 = QVBoxLayout()
        self.HLayout = QHBoxLayout()
        self.Grid = QGridLayout()

        # labels
        self.idLb = QLabel('ID')
        self.codeLb = QLabel('Code')
        self.defLb = QLabel('Definition')
        self.expLb = QLabel('Explanation')

        # combo-boxes
        self.idCb = CboxList(self.columns)
        self.codeCb = CboxList(self.columns)
        self.defCb = CboxList(self.columns)
        self.expCb = CboxList(self.columns)

        # buttons
        self.importQualitiesBttn = QPushButton('Import quality control levels')
        self.closeBttn = QPushButton('Close')
        self.importQualitiesBttn.clicked.connect(self.importMultipleQualities)
        self.closeBttn.clicked.connect(self.closeDialog)

        # set layout
        self.Grid.addWidget(self.idLb, 0, 0)
        self.Grid.addWidget(self.codeLb, 1, 0)
        self.Grid.addWidget(self.defLb, 2, 0)
        self.Grid.addWidget(self.expLb, 3, 0)

        self.Grid.addWidget(self.idCb, 0, 1)
        self.Grid.addWidget(self.codeCb, 1, 1)
        self.Grid.addWidget(self.defCb, 2, 1)
        self.Grid.addWidget(self.expCb, 3, 1)

        self.HLayout.addWidget(self.importQualitiesBttn)
        self.HLayout.addWidget(self.closeBttn)

        self.VLayout.addLayout(self.Grid)
        self.VLayout.addLayout(self.HLayout)

        self.setLayout(self.VLayout)
        self.setMinimumWidth(500)
        self.setWindowTitle('Create multiple new quality control levels')

        # call dialog
        self.exec_()

    # close
    def closeDialog(self):
        self.close()

    # import sites data
    def importMultipleQualities(self):
        session = startDBSession(self.engine)
        register_adapter(np.int64, adapt_numpy_int64)

        # assign variables to data-frame columns
        idCol = self.idCb.currentText()
        codeCol = self.codeCb.currentText()
        defCol = self.defCb.currentText()
        expCol = self.expCb.currentText()
        # add each source
        try:
            for i in self.qualDF.index:
                qualId = int(self.qualDF[idCol][i])
                code = self.qualDF[codeCol][i]
                definition = self.qualDF[defCol][i]
                explanation = self.qualDF[expCol][i]

                quality = QualityControlLevels(QualityControlLevelId=qualId,
                                               QualityControlLevelCode=code,
                                               Definition=definition,
                                               Explanation=explanation)
                session.add(quality)

            session.commit()
            QMessageBox.information(self, 'Database edition', 'Quality control level created!',
                                    QMessageBox.Ok)
        except DataError:
            session.rollback()
            QMessageBox.critical(self, 'Database edition error [DataError]', "Quality control level "
                                 "couldn't be added, make sure provided data fits Controlled "
                                 "Vocabulary (CV) or input types", QMessageBox.Ok)
        except IntegrityError:
            session.rollback()
            QMessageBox.critical(self, 'Database edition error [DataError]', "Quality control level "
                                 "couldn't be added, make sure provided data fits Controlled "
                                 "Vocabulary (CV) or input types", QMessageBox.Ok)


# %% Add Series from different file-types (IDEAM, SIRH, Common vector)
# noinspection PyUnresolvedReferences
class AddSeries(QDialog):
    def __init__(self, engine, parent=None):
        super(AddSeries, self).__init__(parent)

        self.engine = engine
        self.fileNames = None

        # main layout
        self.group1 = QGroupBox('Select source')
        self.group2 = QGroupBox('Data parameters')
        self.group3 = QGroupBox('Source files')
        self.radBttn1 = QRadioButton('IDEAM')
        self.radBttn2 = QRadioButton('SIRH')
        self.radBttn3 = QRadioButton('Generic Vector')
        self.radBttn1.setChecked(True)
        self.HLayout = QHBoxLayout()
        self.HLayout1 = QHBoxLayout()
        self.List = QListWidget()
        self.VLayout = QVBoxLayout()
        self.VLayout1 = QVBoxLayout()
        self.Grid = QGridLayout()

        self.radBttn1.toggled.connect(self.sourceIDEAM)
        self.radBttn2.toggled.connect(self.sourceSIRH)
        self.radBttn3.toggled.connect(self.sourceCommon)

        # get combo-boxes data
        self.sitesDictionary = SqlQuery.get_sites_table(engine)
        self.sourcesDictionary = SqlQuery.get_sources_table(engine)
        self.methodsDictionary = SqlQuery.get_methods_table(engine)
        self.variablesDictionary = SqlQuery.get_vars_table(engine)
        self.qualitiesDictionary = SqlQuery.get_qualities_table(engine)
        self.censorDictionary = SqlQuery.get_censor_table(engine)
        self.unitsDictionary = SqlQuery.get_units_table(engine)
        tUnits = self.variablesDictionary['Time Resolution'][0]

        # labels
        self.sourceLb = QLabel('Source')
        self.methodLb = QLabel('Method')
        self.variableLb = QLabel('Variable')
        self.utcLb = QLabel('Time UTC-Offset')
        self.qualityLb = QLabel('Quality Control Level')
        self.censorLb = QLabel('Censor Code')

        self.sourceDescLb = QLabel(self.sourcesDictionary['Organization'][0])
        self.methodDescLb = QLabel(self.methodsDictionary['Description'][0])
        self.variableDescLb = QLabel(self.variablesDictionary['Variable'][0] + ' - ' +
                                     tUnits + ' - ' + self.variablesDictionary['Type'][0])
        self.qualityDescLb = QLabel(self.qualitiesDictionary['Definition'][0])
        self.censorDescLb = QLabel(self.censorDictionary['Definition'][2])

        self.nSitesLb = QLabel('Number of sites to be imported:')
        self.nVarsLb = QLabel('Variables to be imported:')
        self.nMethodsLb = QLabel('Methods used:')
        self.missSitesLb = QLabel('Missing sites in DB?:')
        self.missVarsLb = QLabel('Missing variables in DB?:')
        self.missMethodsLb = QLabel('Missing methods in DB?:')
        self.importReportLb = QLabel('Report:')
        self.importReportLb.setStyleSheet("font:bold")

        # spin-boxes
        self.utcSb = QSpinBox()
        self.utcSb.setRange(-12, 12)
        self.utcSb.setValue(-5)

        # combo-boxes
        self.sourceCb = CboxList(self.sourcesDictionary['ID'])
        self.methodCb = CboxList(self.methodsDictionary['ID'])
        self.variableCb = CboxList(self.variablesDictionary['ID'])
        self.qualityCb = CboxList(self.qualitiesDictionary['ID'])
        self.censorCb = CboxList(self.censorDictionary['Term'])
        self.censorCb.setCurrentIndex(2)

        self.sourceCb.currentIndexChanged.connect(self.changeSource)
        self.methodCb.currentIndexChanged.connect(self.changeMethod)
        self.variableCb.currentIndexChanged.connect(self.changeVariable)
        self.qualityCb.currentIndexChanged.connect(self.changeQuality)
        self.censorCb.currentIndexChanged.connect(self.changeCensor)

        # buttons
        self.importFilesBttn = QPushButton('Load file paths')
        self.reloadFilesBttn = QPushButton('Reload file paths')
        self.reloadFilesBttn.setEnabled(False)
        self.importSeriesBttn = QPushButton('Import series')
        self.importSeriesBttn.setEnabled(False)
        self.closeBttn = QPushButton('Close')
        self.importFilesBttn.clicked.connect(self.loadFiles)
        self.importSeriesBttn.clicked.connect(self.importSeries)
        self.closeBttn.clicked.connect(self.closeDialog)

        # set layout
        self.Grid.addWidget(self.sourceLb, 0, 0)
        self.Grid.addWidget(self.methodLb, 1, 0)
        self.Grid.addWidget(self.variableLb, 2, 0)
        self.Grid.addWidget(self.qualityLb, 3, 0)
        self.Grid.addWidget(self.censorLb, 4, 0)
        self.Grid.addWidget(self.utcLb, 5, 0)

        self.Grid.addWidget(self.sourceCb, 0, 1)
        self.Grid.addWidget(self.methodCb, 1, 1)
        self.Grid.addWidget(self.variableCb, 2, 1)
        self.Grid.addWidget(self.qualityCb, 3, 1)
        self.Grid.addWidget(self.censorCb, 4, 1)
        self.Grid.addWidget(self.utcSb, 5, 1)

        self.Grid.addWidget(self.sourceDescLb, 0, 2)
        self.Grid.addWidget(self.methodDescLb, 1, 2)
        self.Grid.addWidget(self.variableDescLb, 2, 2)
        self.Grid.addWidget(self.qualityDescLb, 3, 2)
        self.Grid.addWidget(self.censorDescLb, 4, 2)

        self.HLayout.addWidget(self.radBttn1)
        self.HLayout.addWidget(self.radBttn2)
        self.HLayout.addWidget(self.radBttn3)

        self.HLayout1.addWidget(self.importFilesBttn)
        self.VLayout1.addLayout(self.HLayout1)
        self.VLayout1.addWidget(self.List)
        self.VLayout1.addWidget(self.nSitesLb)
        self.VLayout1.addWidget(self.nVarsLb)
        self.VLayout1.addWidget(self.nMethodsLb)
        self.VLayout1.addWidget(self.missSitesLb)
        self.VLayout1.addWidget(self.missVarsLb)
        self.VLayout1.addWidget(self.missMethodsLb)
        self.VLayout1.addWidget(self.importReportLb)
        self.VLayout1.addWidget(self.importSeriesBttn)

        self.group1.setLayout(self.HLayout)
        self.group2.setLayout(self.Grid)
        self.group3.setLayout(self.VLayout1)

        self.VLayout.addWidget(self.group1)
        self.VLayout.addWidget(self.group2)
        self.VLayout.addWidget(self.group3)

        self.setLayout(self.VLayout)
        self.setMinimumWidth(500)
        self.setWindowTitle('Import series from multiple files')

        self.variableCb.setEnabled(False)
        self.variableDescLb.setEnabled(False)
        self.methodCb.setEnabled(False)
        self.methodDescLb.setEnabled(False)

        # call dialog
        self.exec_()

    # disable variables option when IDEAM data source is selected
    def sourceIDEAM(self):
        self.variableCb.setEnabled(False)
        self.variableDescLb.setEnabled(False)
        self.methodCb.setEnabled(False)
        self.methodDescLb.setEnabled(False)
        self.List.clear()

        self.nSitesLb.setText('Number of sites to be imported:')
        self.nVarsLb.setText('Variables to be imported:')
        self.nMethodsLb.setText('Methods used:')
        self.missSitesLb.setText('Missing sites in DB?:')
        self.missVarsLb.setText('Missing variables in DB?:')
        self.missMethodsLb.setText('Missing methods in DB?:')
        self.importReportLb.setText('Report:')

        self.importSeriesBttn.setEnabled(False)

    # enable variables option when SIRH data source is selected
    def sourceSIRH(self):
        self.variableCb.setEnabled(True)
        self.variableDescLb.setEnabled(True)
        self.methodCb.setEnabled(True)
        self.methodDescLb.setEnabled(True)
        self.List.clear()

        self.nSitesLb.setText('Number of sites to be imported:')
        self.nVarsLb.setText('Variables to be imported:')
        self.nMethodsLb.setText('Methods used:')
        self.missSitesLb.setText('Missing sites in DB?:')
        self.missVarsLb.setText('Missing variables in DB?:')
        self.missMethodsLb.setText('Missing methods in DB?:')
        self.importReportLb.setText('Report:')

        self.importSeriesBttn.setEnabled(False)

    # enable variables option when Common Vector data source is selected
    def sourceCommon(self):
        self.variableCb.setEnabled(True)
        self.variableDescLb.setEnabled(True)
        self.methodCb.setEnabled(True)
        self.methodDescLb.setEnabled(True)
        self.List.clear()

        self.nSitesLb.setText('Number of sites to be imported:')
        self.nVarsLb.setText('Variables to be imported:')
        self.nMethodsLb.setText('Methods used:')
        self.missSitesLb.setText('Missing sites in DB?:')
        self.missVarsLb.setText('Missing variables in DB?:')
        self.missMethodsLb.setText('Missing methods in DB?:')
        self.importReportLb.setText('Report:')

        self.importSeriesBttn.setEnabled(False)

    # change labels when combo-box is changed
    def changeSource(self):
        index = self.sourcesDictionary['ID'].index(int(self.sourceCb.currentText()))
        label = self.sourcesDictionary['Organization'][index]
        self.sourceDescLb.setText(label)

    def changeMethod(self):
        index = self.methodsDictionary['ID'].index(int(self.methodCb.currentText()))
        label = self.methodsDictionary['Description'][index]
        self.methodDescLb.setText(label)

    def changeVariable(self):
        index = self.variablesDictionary['ID'].index(int(self.variableCb.currentText()))
        label = (self.variablesDictionary['Variable'][index] + ' - ' +
                 self.variablesDictionary['Time Resolution'][index] + ' - ' +
                 self.variablesDictionary['Type'][index])
        self.variableDescLb.setText(label)

    def changeQuality(self):
        index = self.qualitiesDictionary['ID'].index(int(self.qualityCb.currentText()))
        label = self.qualitiesDictionary['Definition'][index]
        self.qualityDescLb.setText(label)

    def changeCensor(self):
        index = self.censorDictionary['Term'].index(self.censorCb.currentText())
        label = self.censorDictionary['Definition'][index]
        self.censorDescLb.setText(label)

    # close
    def closeDialog(self):
        self.close()

    def loadFiles(self):
        self.List.clear()
        dlg = QFileDialog()
        # use last working directory
        dlg.setFileMode(QFileDialog.ExistingFile)
        caption = 'Open files'

        if self.radBttn1.isChecked():
            filter_mask = "Text files (*.txt)"
        else:
            filter_mask = "CSV/XLS files (*.csv *xls *xlsx)"
        # noinspection PyTypeChecker
        self.fileNames, _ = dlg.getOpenFileNames(None, caption, None, filter_mask)
        for i in self.fileNames:
            filename = i.split('/')
            filename = filename[-1]
            self.List.addItem(filename)

        if len(self.fileNames) > 0:
            filesReport = expIDEAMFiles(self.fileNames, self.methodsDictionary, self.variablesDictionary,
                                        self.sitesDictionary)
            self.nSitesLb.setText('Number of sites to be imported: ' + str(filesReport[0]))
            self.nVarsLb.setText('Variables to be imported: ' + str(filesReport[1]))
            self.nMethodsLb.setText('Methods used :' + str(filesReport[2]))

            if filesReport[5]:
                self.missVarsLb.setText('Missing variables in DB?: No')
            else:
                self.missVarsLb.setText('Missing variables in DB?: Yes')
            if filesReport[6]:
                self.missMethodsLb.setText('Missing methods in DB?: No')
            else:
                self.missMethodsLb.setText('Missing methods in DB?: Yes')
            if filesReport[7]:
                self.missSitesLb.setText('Missing sites in DB?: No')
            else:
                self.missSitesLb.setText('Missing sites in DB?: Yes')

            if filesReport[5] and filesReport[6] and filesReport[7]:
                self.importReportLb.setText('Report: Proceed to import series')
                self.importSeriesBttn.setEnabled(True)
            elif filesReport[5] and filesReport[6] and not filesReport[7]:
                self.importReportLb.setText('Report: Create missing site in database, then reload filelist')
                self.importSeriesBttn.setEnabled(False)
            elif filesReport[5] and not filesReport[6] and filesReport[7]:
                self.importReportLb.setText('Report: Create missing method in database, then reload filelist')
                self.importSeriesBttn.setEnabled(False)
            elif not filesReport[5] and filesReport[6] and filesReport[7]:
                self.importReportLb.setText('Report: Create missing variable in database, then reload filelist')
                self.importSeriesBttn.setEnabled(False)
            elif not filesReport[5] and not filesReport[6] and filesReport[7]:
                self.importReportLb.setText('Report: Create missing variable and method in database, '
                                            'then reload filelist')
                self.importSeriesBttn.setEnabled(False)
            elif not filesReport[5] and filesReport[6] and not filesReport[7]:
                self.importReportLb.setText('Report: Create missing site and variable in database, '
                                            'then reload filelist')
                self.importSeriesBttn.setEnabled(False)
            elif filesReport[5] and not filesReport[6] and not filesReport[7]:
                self.importReportLb.setText('Report: Create missing site and method in database, '
                                            'then reload filelist')
                self.importSeriesBttn.setEnabled(False)
            else:
                self.importReportLb.setText('Report: Create missing site, variable and method in database, '
                                            'the reload filelist')
                self.importSeriesBttn.setEnabled(False)

    def importSeries(self):
        for i in self.fileNames:
            importIDEAMDaily(i, self.engine, self.methodsDictionary, self.variablesDictionary,
                             np.int(self.sourceCb.currentText()), np.int(self.qualityCb.currentText()),
                             self.censorCb.currentText(), np.float(-5))
