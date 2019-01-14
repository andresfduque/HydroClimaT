#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
# Created by AndresD at 13/11/18

Features:
    + Databases creation and edition
    + Hydrology and climate time series management
    + Series processing, statistics and advanced analysis

@author:    Andres Felipe Duque Perez
Email:      andresfduque@gmail.com
"""
# TODO: add GIS support
# TODO: add morphometric analysis

# %%  Main imports
import sys
import TableViews
import DatabaseEditor
import pandas as pd
import pyqtgraph as pg
import ManageDatabases as myDB

from HomeMenu import HomeWidget, PostgresForm

from PyQt5.QtGui import QIcon, QKeySequence
from PyQt5.QtCore import pyqtSlot, pyqtSignal, Qt, QFileInfo, QFile, QTextStream
from PyQt5.QtWidgets import (QGridLayout, QHBoxLayout, QVBoxLayout, QWidget, QMessageBox, QTabWidget, QMainWindow,
                             QToolBar, QApplication, QTextEdit, QAction, QFileDialog, QDockWidget, QStyleFactory,
                             QLabel)

import qrc_resources


# Project management toolbar for home ribbon
class ProjectToolbar(QToolBar):

    def __init__(self, parent=None):
        super(ProjectToolbar, self).__init__(parent)

        # define default class instances
        self.curFile = None                                                 # current file

        # define toolbar actions [new project]
        self.newAct = QAction(QIcon(':/new_project'), '&New', self)
        self.newAct.setStatusTip('Open existing project')
        self.newAct.setShortcut(QKeySequence.Open)
        self.newAct.triggered.connect(self.open)

        # define toolbar actions [open project]
        self.openAct = QAction(QIcon(':/open_project'), '&Open...', self)
        self.openAct.setStatusTip('Open existing project')
        self.openAct.setShortcut(QKeySequence.Open)
        self.openAct.triggered.connect(self.open)

        # define toolbar actions [save project]
        self.saveAct = QAction(QIcon(':/save_project'), '&Save', self)
        self.saveAct.setStatusTip('Save project')
        self.saveAct.setShortcut(QKeySequence.Save)
        self.saveAct.triggered.connect(self.save)

        # define toolbar actions [save project as]
        self.saveAsAct = QAction(QIcon(':/save_project_as'), '&Save as . . .', self)
        self.saveAsAct.setStatusTip('Save project as...')
        self.saveAsAct.setShortcut(QKeySequence.SaveAs)
        self.saveAsAct.triggered.connect(self.saveAs)

        #        self.exitAct = QAction("E&xit", self, shortcut="Ctrl+Q", statusTip="Exit the application",
        #                               triggered=self.close)
        #
        #        self.aboutAct = QAction("&About", self, statusTip="Show the application's About box",
        #                                triggered=self.about)
        #
        #        self.aboutQtAct = QAction("About &Qt", self, statusTip="Show the Qt library's About box",
        #                                  triggered=QApplication.instance().aboutQt)

        # add actions to toolbar
        self.addAction(self.newAct)
        self.addAction(self.openAct)
        self.addAction(self.saveAct)
        self.addAction(self.saveAsAct)
        self.setFixedHeight(40)
        self.setWindowTitle('Project')

    # def closeEvent(self, event):
    #     if self.maybeSave():
    #         self.writeSettings()
    #         event.accept()
    #     else:
    #         event.ignore()

    def new_file(self):
        if self.maybeSave():
            self.textEdit.clear()
            self.setCurrentFile('')

    def open(self):
        if self.maybeSave():
            file_name, _ = QFileDialog.getOpenFileName(self)
            if file_name:
                self.loadFile(file_name)

    def save(self):
        if self.curFile:
            return self.saveFile(self.curFile)

        return self.saveAs()

    def saveAs(self):
        file_name, _ = QFileDialog.getSaveFileName(self)
        if file_name:
            return self.saveFile(file_name)

        return False

    def about(self):
        QMessageBox.about(self, "About Application",
                          "The <b>Application</b> example demonstrates how to write "
                          "modern GUI applications using Qt, with a menu bar, "
                          "toolbars, and a status bar.")

    def documentWasModified(self):
        self.setWindowModified(self.textEdit.document().isModified())

    def maybeSave(self):
        if self.textEdit.document().isModified():
            ret = QMessageBox.warning(self, "Application", "The document has been modified.\
                                      \nDo you want to save your changes?",
                                      QMessageBox.Save | QMessageBox.Discard | QMessageBox.Cancel)

            if ret == QMessageBox.Save:
                return self.save()

            if ret == QMessageBox.Cancel:
                return False

        return True

    def loadFile(self, file_name):
        file = QFile(file_name)
        if not file.open(QFile.ReadOnly | QFile.Text):
            QMessageBox.warning(self, "Application",
                                "Cannot read file %s:\n%s." % (file_name, file.errorString()))
            return

        inf = QTextStream(file)
        QApplication.setOverrideCursor(Qt.WaitCursor)
        self.textEdit.setPlainText(inf.readAll())
        QApplication.restoreOverrideCursor()

        self.setCurrentFile(file_name)
        self.statusBar().showMessage("File loaded", 2000)

    def saveFile(self, file_name):
        file = QFile(file_name)
        if not file.open(QFile.WriteOnly | QFile.Text):
            QMessageBox.warning(self, "Application",
                                "Cannot write file %s:\n%s." % (file_name, file.errorString()))
            return False

        return True

    def setCurrentFile(self, file_name):
        self.curFile = file_name
        self.textEdit.document().setModified(False)
        self.setWindowModified(False)

        if self.curFile:
            shownName = self.strippedName(self.curFile)
        else:
            shownName = 'untitled.txt'

        self.setWindowTitle("%s[*] - Application" % shownName)

    @staticmethod
    def strippedName(full_file_name):
        return QFileInfo(full_file_name).fileName()


# Database connection manager toolbar for home ribbon
# noinspection PyUnresolvedReferences
class DatabaseToolbar(QToolBar):
    connDictionary = {}                                     # dictionary to store connection parameters
    connSignal = pyqtSignal(object)                         # connection signal to send connDictionary

    def __init__(self, parent=None):
        super(DatabaseToolbar, self).__init__(parent)

        # create toolbar actions [connect to postgres database]
        postgresDB = QAction(QIcon(':/postgres_db'), '&Postgres', self)
        postgresDB.setStatusTip('Connect to PostgreSQL database')
        postgresDB.setShortcut(QKeySequence.New)
        postgresDB.triggered.connect(self.connPostgres)

        # create toolbar actions [connect to sqlite database]
        sqliteDB = QAction(QIcon(':/sqlite_db'), '&Sqlite', self)
        sqliteDB.setStatusTip('Connect to SQLite database')
        sqliteDB.setShortcut(QKeySequence.Open)
        sqliteDB.triggered.connect(self.connSqlite)

        # set database toolbar
        self.setFixedHeight(40)
        self.addAction(postgresDB)
        self.addAction(sqliteDB)
        self.setWindowTitle('Database connection')

        # define default class instances
        self.postgresForm = None

    # postgres database connection - pass connection dictionary to main application
    def connPostgres(self):
        self.postgresForm = PostgresForm()
        self.postgresForm.connDict.connect(self.addConn)
        self.postgresForm.exec_()

    # test sqlite database connection - pass connection to main window dictionary
    def connSqlite(self):
        self.postgresForm = PostgresForm()
        self.postgresForm.exec_()

    # slot to receive database connection parameters and emit the same parameters to main-window
    @pyqtSlot(object)
    def addConn(self, conn_dict):
        # emit connection parameters to database dock-widget
        self.connSignal.emit(conn_dict)


# Home Ribbon Widget
class TabMenu(QTabWidget):
    def __init__(self, parent=None):
        super(TabMenu, self).__init__(parent)

        self.homeMainWindow = HomeWidget()          # main-window to add home toolbars
        self.addTab(self.homeMainWindow, 'Home')    # add homeMainWindow to a tab
        self.setTabShape(0)

# Workspace widget for time-series processing
class TimeseriesWorkspace(QWidget):
    def __init__(self, parent=None):
        super(TimeseriesWorkspace, self).__init__(parent)

        self.label = QLabel('Workspace')
        self.label.setStyleSheet('background-color: rgb(158,162,170); border-radius: 3px')
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


# Main application
# noinspection PyUnresolvedReferences
class HydroClimate(QMainWindow):

    connDictionary = {}         # dictionary to store connection parameters
    tabTableView = None         # table view of database tables, also management of data

    timeSeriesDict = {}         # dictionary to store a single site query (related data)
    timeSeriesDf = pd.DataFrame()
    timeSeriesId = 0
    stackedId = 0               # index to identify the stacked workspace

    def __init__(self, parent=None):
        super(HydroClimate, self).__init__(parent)

        # define default class instances
        self.err = None
        self.meta = None
        self.tsCode = None
        self.tsName = None
        self.dbBase = None
        self.engine = None
        self.newForm = None
        self.connName = None
        self.timeseriesPlot = None
        self.timeseriesWorkspace = None
        self.timeseriesVectorWidget = None
        self.setContentsMargins(10, 10, 10, 10)

        # main window layout
        self.mainGrid = QGridLayout()                                   # layout of main window
        self.widget = QWidget(self)                                     # widget to set main window layout
        self.VLayout = QVBoxLayout()                                    # layout for database management widgets
        self.tabMenu = TabMenu()                                        # tab-widget for toolbars
        # self.tabMenu.databaseToolbar.connSignal.connect(self.addDBConn)   # add database connection
        # self.workspace = HomeWidget()                                    # widget to display time-series analysis

        # self.workspace.treeWidget.setHidden(True)
        # self.workspace.stackedWorkspace.addWidget(TimeseriesWorkspace())
        # self.workspace.connButton.clicked.connect(self.connDatabase)
        # self.workspace.delConnButton.clicked.connect(self.deleteConnection)
        # self.workspace.treeWidget.workspaceTimeSeries.connect(self.getTimeSeries)
        # self.workspace.treeWidget.changeWorkspace.connect(self.changeWorkspace)

        self.VLayout.addWidget(self.tabMenu)
        self.VLayout.addWidget(QTextEdit())
        self.mainGrid.addWidget(self.tabMenu, 0, 0)

        self.mainGrid.setAlignment(Qt.AlignTop)
        # self.mainGrid.addWidget(self.workspace, 1, 0)
        self.widget.setLayout(self.mainGrid)

        self.setCentralWidget(self.tabMenu)
#        self.busyIndicator = QtWaitingSpinner(self.centralWidget())
        self.setWindowTitle("Hydro-ClimaT")
        self.move(200, 100)

        self.show()

    # connect database either postgres or sqlite
    def connDatabase(self):
        if self.workspace.dbCombobox.currentText() != '':
            self.connName = self.workspace.dbCombobox.currentText()
            connParameters = self.connDictionary[self.connName]
            user = connParameters['user']
            password = connParameters['password']
            db_name = connParameters['db_name']
            host = connParameters['host']
            port = connParameters['port']
            driver = connParameters['driver']
            if driver == 'postgres':
                self.engine, self.meta, self.err = myDB.psql_conn(user, password, db_name, host, port)

            elif driver == 'sqlite':    # correct to add sqlite support
                self.engine, self.meta, self.err = myDB.psql_conn(user, password, db_name, host, port)

            # tab-widget to show database structure
            if self.tabTableView is not None:
                self.tabTableView.deleteLater()
                self.DbTableViews(self.engine)
                self.workspace.VLayout1.addWidget(self.tabTableView)
            else:
                self.DbTableViews(self.engine)
                # noinspection PyTypeChecker
                self.workspace.VLayout1.addWidget(self.tabTableView)

            # disable the connection button, enable connection deletion button
            self.workspace.connButton.setEnabled(False)
            self.workspace.delConnButton.setEnabled(True)

    # delete database connection
    def deleteConnection(self):
        if self.workspace.dbCombobox.currentText() != '':
            self.connName = self.workspace.dbCombobox.currentText()
            self.workspace.dbCombobox.removeItem(self.workspace.dbCombobox.currentIndex())
            del self.connDictionary[self.connName]

            self.tabTableView.deleteLater()
            self.tabTableView = None

            # enable the connection button, disable connection deletion button
            self.workspace.connButton.setEnabled(True)
            self.workspace.delConnButton.setEnabled(False)

    # tab-widget to show database structure
    def DbTableViews(self, engine):
        self.tabTableView = TableViews.DbTabView(engine)

        # table edition dialogs
        self.tabTableView.sitesTable.workspaceSite.connect(self.querySite)                  # query site
        self.tabTableView.sitesTable.createNewSite.connect(self.createSite)                 # create new site
        self.tabTableView.metadataTable.createNewMetadata.connect(self.createMetadata)      # create new metadata
        self.tabTableView.sourcesTable.createNewSource.connect(self.createSource)           # create new source
        self.tabTableView.variablesTable.createNewVariable.connect(self.createVariable)     # create new variable
        self.tabTableView.methodsTable.createNewMethod.connect(self.createMethod)           # create new method
        self.tabTableView.qualitiesTable.createNewQuality.connect(self.createQuality)       # create new quality
        self.tabTableView.qualifiersTable.createNewQualifier.connect(self.createQualifier)  # create new qualifier
        self.tabTableView.sitesTable.importNewSeries.connect(self.importSeries)             # import series

    # receive database connection parameters
    @pyqtSlot(object)
    def addDBConn(self, conn_dict):
        if not conn_dict['connName'] in self.connDictionary:
            self.connDictionary[conn_dict['connName']] = conn_dict
            self.workspace.dbCombobox.addItem(conn_dict['connName'])

    # receive site to be consulted
    @pyqtSlot(object)
    def querySite(self, workspace_site):
        self.tsCode = workspace_site[0]
        self.tsName = workspace_site[1]
        self.dbBase = DatabaseEditor.DbExplorer(workspace_site, self.engine)
        self.dbBase.timeSeriesIdentifier.connect(self.addSeriesToWorkspace)

    # create new site
    @pyqtSlot(object)
    def createSite(self):
        # improve table view updates
        self.workspace.VLayout1.removeWidget(self.tabTableView)
        self.newForm = DatabaseEditor.AddSite(self.engine)
        self.DbTableViews(self.engine)
        self.workspace.VLayout1.addWidget(self.tabTableView)
        self.tabTableView.setCurrentIndex(0)

    # create new metadata
    @pyqtSlot(object)
    def createMetadata(self):
        self.workspace.VLayout1.removeWidget(self.tabTableView)
        self.newForm = DatabaseEditor.AddMetadata(self.engine)
        self.DbTableViews(self.engine)
        self.workspace.VLayout1.addWidget(self.tabTableView)
        self.tabTableView.setCurrentIndex(1)

    # create new source
    @pyqtSlot(object)
    def createSource(self):
        self.workspace.VLayout1.removeWidget(self.tabTableView)
        self.newForm = DatabaseEditor.AddSource(self.engine)
        self.DbTableViews(self.engine)
        self.workspace.VLayout1.addWidget(self.tabTableView)
        self.tabTableView.setCurrentIndex(2)

    # create new variable
    @pyqtSlot(object)
    def createVariable(self):
        self.workspace.VLayout1.removeWidget(self.tabTableView)
        self.newForm = DatabaseEditor.AddVariable(self.engine)
        self.DbTableViews(self.engine)
        self.workspace.VLayout1.addWidget(self.tabTableView)
        self.tabTableView.setCurrentIndex(3)

    # create new method
    @pyqtSlot(object)
    def createMethod(self):
        self.workspace.VLayout1.removeWidget(self.tabTableView)
        self.newForm = DatabaseEditor.AddMethod(self.engine)
        self.DbTableViews(self.engine)
        self.workspace.VLayout1.addWidget(self.tabTableView)
        self.tabTableView.setCurrentIndex(4)

    # create new method
    @pyqtSlot(object)
    def createQuality(self):
        self.workspace.VLayout1.removeWidget(self.tabTableView)
        self.newForm = DatabaseEditor.AddQuality(self.engine)
        self.DbTableViews(self.engine)
        self.workspace.VLayout1.addWidget(self.tabTableView)
        self.tabTableView.setCurrentIndex(5)

    # create new qualifier
    @pyqtSlot(object)
    def createQualifier(self):
        self.workspace.VLayout1.removeWidget(self.tabTableView)
        self.newForm = DatabaseEditor.AddQualifier(self.engine)
        self.DbTableViews(self.engine)
        self.workspace.VLayout1.addWidget(self.tabTableView)
        self.tabTableView.setCurrentIndex(6)

    # import series
    @pyqtSlot(int)
    def importSeries(self):
        self.workspace.VLayout1.removeWidget(self.tabTableView)
        self.newForm = DatabaseEditor.AddSeries(self.engine)
        self.DbTableViews(self.engine)
        self.workspace.VLayout1.addWidget(self.tabTableView)
        self.tabTableView.setCurrentIndex(0)

    # add series to be analyzed to tree-widget
    @pyqtSlot(object)
    def addSeriesToWorkspace(self, time_series_identifier):
        time_series_identifier.append([str(self.tsCode), self.tsName])    # add code and name to identifier
        time_series_identifier.append(0)                                  # stacked workspace index

        # check if added time-series already exists
        if time_series_identifier not in self.timeSeriesDict.values():
            self.timeSeriesDict[self.timeSeriesId] = time_series_identifier   # add identifier to dictionary

            # check if tree-widget already exist
            if self.workspace.treeWidget.isHidden():
                self.workspace.treeWidget.setHidden(False)
                self.workspace.treeWidget.addSite(self.timeSeriesId, time_series_identifier)
            else:
                self.workspace.treeWidget.addSite(self.timeSeriesId, time_series_identifier)

            self.timeSeriesId += 1

        # connect to getTimeSeries
#        print(self.timeSeriesDict)
#        self.treeWidget.workspaceTimeSeries.connect(self.getTimeSeries)

    # get specific time-series from database
#     @pyqtSlot(object)
#     def getTimeSeries(self, ts_id):
#
#         self.stackedId += 1
#
#         searchParameters = self.timeSeriesDict[int(ts_id)]
#         print(self.timeSeriesDict)
#         timeseries = sqlQuery.timeSeriesQuery(searchParameters, self.engine)
#         dates = timeseries.index
#         timeseriesVector = TableViews.GenericTableView({'Date': dates,
#                                                         'Value': timeseries.values},
#                                                        ['Date', 'Value'], [1, 2])
#
#         self.timeseriesWorkspace = TimeseriesWorkspace()
#         self.timeseriesWorkspace.label.setText('Site: ' + searchParameters[4][0] + ' / Variable: ' +
#                                                searchParameters[1] + ' / Source: ' + searchParameters[0])
#         self.timeseriesVectorWidget = QWidget()
#         self.timeseriesVectorWidget.setLayout(timeseriesVector)
#         self.timeseriesWorkspace.tabWorkspace.addTab(self.timeseriesVectorWidget, 'Timeseries [Vector]')
#
#         self.workspace.stackedWorkspace.addWidget(self.timeseriesWorkspace)
#         self.workspace.stackedWorkspace.setCurrentIndex(self.stackedId)
#         self.timeSeriesDict[int(ts_id)][5] = self.stackedId
#
#         self.timeseriesPlot = pg.PlotWidget()
# #        self.timeseriesPlot
# #        print(dates)
# #        self.timeseriesPlot.plot(dates, timeseries.values)
#         self.timeseriesWorkspace.dockGraphs.setWidget(self.timeseriesPlot)
#
# #        self.dockGraphs.setWidget(self.timeseriesPlot)
# #        timeseries.plot(dates, timeseries.values)

    @pyqtSlot(object)
    def changeWorkspace(self, ts_id):
        i = self.timeSeriesDict[int(ts_id)][5]
        self.workspace.stackedWorkspace.setCurrentIndex(i)


# run application
if __name__ == '__main__':

    if sys.platform == 'win32':
        QApplication.setStyle(QStyleFactory.create('fusion'))
    elif sys.platform == 'linux':
        QApplication.setStyle(QStyleFactory.create('cleanlooks'))

    app = QApplication(sys.argv)
    dialog = HydroClimate()
    sys.exit(app.exec_())
