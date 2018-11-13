# -*- coding: utf-8 -*-
"""
Created on Sun Jul  2 21:29:31 2017

MANAGE DATABASES CONNECTIONS:
    + Connect to an environmental database
        - access to postgres or sqlite3 (under construction) database
        - get database structure
    + Connect to projects database
        - (under construction)
    + Connect to budgets database
        - (under construction)
    + Connect to finances database
        - (under construction)

REQUIREMENTS:
    + PostgreSQL 10.1 or SQLITE3
    + psycopg2 [python module]
    + SQL Alchemy [python module]

@author:    Andrés Felipe Duque Pérez
email:      andresfduque@gmail.com
"""

# %% Main imports

import SQLAlchemyQueries as SqlQuery
from PyQt5.QtCore import QSortFilterProxyModel, Qt, pyqtSignal
from PyQt5.QtGui import QStandardItemModel, QStandardItem, QColor, QBrush, QCursor, QFont
from PyQt5.QtWidgets import (QComboBox, QHBoxLayout, QLineEdit, QVBoxLayout, QTableView,
                             QAbstractScrollArea, QMenu, QAction, QTabWidget, QWidget, QTreeWidget,
                             QTreeWidgetItem, QTreeWidgetItemIterator, QHeaderView)


# %% Generic table view
class GenericTableView(QVBoxLayout):
    """
    Create table-view to display in Hydro-ClimaT:
        dictionary: {'var1': data1, 'var2': data2} ; data must have same size \n
        header: column names as a vector of strings; must have same elements as dictionary vars \n
        alignment: vector of integers indicating each column alignment; 0:left, 1:center, 2:right
    """
    def __init__(self, dictionary, header, col_alignment, parent=None):
        super(GenericTableView, self).__init__(parent)

        self.tableView = QTableView()

        # special fonts
        self.tableViewFont = QFont()
        self.tableViewFont.setPointSize(9)

        # standard item model
        dictKeys = dictionary.keys()
        firstKey = list(dictionary.keys())[0]
        cols = len(dictKeys)
        rows = len(dictionary[firstKey])
        self.model = QStandardItemModel(rows, cols)
        self.model.setHorizontalHeaderLabels(header)

        for i in range(0, len(header)):
            self.model.setHeaderData(i, Qt.Horizontal, QBrush(QColor('#B0C4DE')), Qt.BackgroundRole)

        j = 0
        for col in dictionary.keys():
            i = 0
            for row in dictionary[col]:
                if col == 'Date':
                    item = QStandardItem(row.strftime('%Y-%m-%d'))
                elif col == 'Latitude' or col == 'Longitude':
                    item = QStandardItem(str(format(row, '0.5f')))
                else:
                    item = QStandardItem(str(row))

                item.setEditable(False)
                alignment = col_alignment[j]
                if alignment == 0:
                    item.setTextAlignment(Qt.AlignVCenter | Qt.AlignLeft)
                elif alignment == 1:
                    item.setTextAlignment(Qt.AlignVCenter | Qt.AlignCenter)
                elif alignment == 2:
                    item.setTextAlignment(Qt.AlignVCenter | Qt.AlignRight)

                self.model.setItem(i, j, item)
                i += 1
            j += 1

        # filter proxy model
        self.filter_proxy_model = QSortFilterProxyModel()
        self.filter_proxy_model.setSourceModel(self.model)
        self.filter_proxy_model.setFilterKeyColumn(0)   # first column

        # headers properties
        self.hh = self.tableView.horizontalHeader()
        self.vh = self.tableView.verticalHeader()
        self.vh.setVisible(False)

        self.hh.setResizeMode(QHeaderView.Fixed)
        self.vh.setResizeMode(QHeaderView.Fixed)
        self.hh.setFont(self.tableViewFont)
        self.hh.setFixedHeight(18)

        # table view properties
        self.tableView.setSortingEnabled(True)
        self.tableView.setModel(self.filter_proxy_model)
        self.tableView.setSizeAdjustPolicy(QAbstractScrollArea.AdjustToContents)
        self.tableView.setFont(self.tableViewFont)
        self.tableView.setContextMenuPolicy(Qt.CustomContextMenu)

        self.tableView.resizeColumnsToContents()
        self.tableView.resizeRowsToContents()

        # set widget to layout
        self.addWidget(self.tableView)


# %% Sites table view
# noinspection PyUnresolvedReferences
class SitesTable(GenericTableView):

    # signal to pass dictionary to databases dock-widget
    workspaceSite = pyqtSignal(object)
    createNewSite = pyqtSignal(object)
    importNewSeries = pyqtSignal(int)

    # define default class instances
    code = None
    name = None

    def __init__(self, dictionary, header, col_alignment):
        super(SitesTable, self).__init__(dictionary, header, col_alignment)

        # table view
        self.tableView.customContextMenuRequested.connect(self.openMenu)
        self.tableView.verticalHeader().setDefaultSectionSize(18)

        # search parameters
        self.HLayout = QHBoxLayout()
        self.search_le = QLineEdit()
        self.search_le.textChanged.connect(self.filter_proxy_model.setFilterRegExp)
        self.search_cb = QComboBox()
        self.search_cb.addItem('Code')
        self.search_cb.addItem('Name')
        self.search_cb.currentIndexChanged.connect(self.cbEventChange)

        # layout
        self.HLayout.addWidget(self.search_le)
        self.HLayout.addWidget(self.search_cb)
        self.addLayout(self.HLayout)
        self.search_par = self.search_cb.currentText()

        self.menu = QMenu()
        self.sitesMenu = QMenu('Sites')
        self.timeseriesMenu = QMenu('Timeseries')
        self.addSite = QAction('Add new site (single/multiple)...', self)
        self.showSite = QAction('Show site', self)
        self.deleteSite = QAction('Delete site', self)
        self.editSite = QAction('Edit site', self)

        self.addTimeseries = QAction('Import timeseries (single/multiple)...', self)
        self.deleteTimeseries = QAction('Delete timeseries...', self)
        self.timeseriesQuery = QAction('Timeseries query', self)

    def cbEventChange(self):
        if self.search_par == 'Code':
            self.filter_proxy_model.setFilterKeyColumn(0)   # first column
        elif self.search_par == 'Name':
            self.filter_proxy_model.setFilterKeyColumn(1)   # second column

    # right click menu
    def openMenu(self):
        row = self.filter_proxy_model.mapToSource(self.tableView.selectionModel().currentIndex()).row()
        codeIndex = self.model.index(row, 0)
        nameIndex = self.model.index(row, 1)
        self.code = self.model.data(codeIndex)
        self.name = self.model.data(nameIndex)

        # drop menu [sites menu and timeseries menu]
        self.timeseriesQuery.triggered.connect(self.emitSite)
        self.addSite.triggered.connect(self.createSite)
        self.addTimeseries.triggered.connect(self.importSeries)

        self.sitesMenu.addAction(self.addSite)
        self.sitesMenu.addAction(self.showSite)
        self.sitesMenu.addAction(self.deleteSite)
        self.sitesMenu.addSeparator()
        self.sitesMenu.addAction(self.editSite)

        self.timeseriesMenu.addAction(self.addTimeseries)
        self.timeseriesMenu.addAction(self.deleteTimeseries)
        self.timeseriesMenu.addSeparator()
        self.timeseriesMenu.addAction(self.timeseriesQuery)

        self.menu.addMenu(self.sitesMenu)
        self.menu.addMenu(self.timeseriesMenu)

        self.menu.popup(QCursor.pos())

    def emitSite(self):
        # emit site to be consulted in the workspace
        self.workspaceSite.emit([self.code, self.name])

    def createSite(self):
        self.createNewSite.emit(1)

    def importSeries(self):
        self.importNewSeries.emit(1)


# %% Metadata table view
# noinspection PyUnresolvedReferences
class MetadataTable(GenericTableView):

    # signal to pass dictionary to databases dock-widget
    createNewMetadata = pyqtSignal(object)

    # define default class instances
    metadataMenu = None
    addMetadata = None
    deleteMetadata = None

    def __init__(self, dictionary, header, col_alignment):
        super(MetadataTable, self).__init__(dictionary, header, col_alignment)

        # table view
        self.tableView.customContextMenuRequested.connect(self.openMenu)
        self.tableView.verticalHeader().setDefaultSectionSize(18)

    # right click menu
    def openMenu(self):
        # drop menu [sites menu and timeseries menu]
        self.metadataMenu = QMenu('Metadata')
        self.addMetadata = QAction('Add metadata (single)...', self)
        self.deleteMetadata = QAction('Delete metadata', self)

        self.addMetadata.triggered.connect(self.createMetadata)

        self.metadataMenu.addAction(self.addMetadata)
        self.metadataMenu.addAction(self.deleteMetadata)

        self.metadataMenu.popup(QCursor.pos())

    def createMetadata(self):
        self.createNewMetadata.emit(1)


# %% Sources table view
# noinspection PyUnresolvedReferences
class SourcesTable(GenericTableView):

    # signal to pass dictionary to databases dock-widget
    createNewSource = pyqtSignal(object)

    # define default class instances
    sourcesMenu = None
    addSource = None
    deleteSource = None

    def __init__(self, dictionary, header, col_alignment):
        super(SourcesTable, self).__init__(dictionary, header, col_alignment)

        # table view
        self.tableView.customContextMenuRequested.connect(self.openMenu)
        self.tableView.verticalHeader().setDefaultSectionSize(18)

    # right click menu
    def openMenu(self):
        # drop menu [sites menu and timeseries menu]
        self.sourcesMenu = QMenu('Sources')
        self.addSource = QAction('Add source (single/multiple)...', self)
        self.deleteSource = QAction('Delete source', self)

        self.addSource.triggered.connect(self.createSource)

        self.sourcesMenu.addAction(self.addSource)
        self.sourcesMenu.addAction(self.deleteSource)

        self.sourcesMenu.popup(QCursor.pos())

    def createSource(self):
        self.createNewSource.emit(1)


# %% Variables table view
# noinspection PyUnresolvedReferences
class VariablesTable(GenericTableView):

    # signal to pass dictionary to databases dock-widget
    createNewVariable = pyqtSignal(object)

    # define default class instances
    variablesMenu = None
    addVariable = None
    deleteVariable = None

    def __init__(self, dictionary, header, col_alignment):
        super(VariablesTable, self).__init__(dictionary, header, col_alignment)

        # table view
        self.tableView.customContextMenuRequested.connect(self.openMenu)
        self.tableView.verticalHeader().setDefaultSectionSize(18)

    # right click menu
    def openMenu(self):
        # drop menu [sites menu and time-series menu]
        self.variablesMenu = QMenu('Variables')
        self.addVariable = QAction('Add variable (single/multiple)...', self)
        self.deleteVariable = QAction('Delete variable', self)

        self.addVariable.triggered.connect(self.createVariable)

        self.variablesMenu.addAction(self.addVariable)
        self.variablesMenu.addAction(self.deleteVariable)

        self.variablesMenu.popup(QCursor.pos())

    def createVariable(self):
        self.createNewVariable.emit(1)


# %% Methods table view
# noinspection PyUnresolvedReferences,PyUnresolvedReferences
class MethodsTable(GenericTableView):

    # signal to pass dictionary to databases dock-widget
    createNewMethod = pyqtSignal(object)

    # define default class instances
    methodsMenu = None
    addMethod = None
    deleteMethod = None

    def __init__(self, dictionary, header, col_alignment):
        super(MethodsTable, self).__init__(dictionary, header, col_alignment)

        # table view
        self.tableView.customContextMenuRequested.connect(self.openMenu)
        self.tableView.verticalHeader().setDefaultSectionSize(18)

    # right click menu
    def openMenu(self):
        # drop menu [sites menu and timeseries menu]
        self.methodsMenu = QMenu('Methods')
        self.addMethod = QAction('Add method (single/multiple)...', self)
        self.deleteMethod = QAction('Delete method', self)

        self.addMethod.triggered.connect(self.createMethod)

        self.methodsMenu.addAction(self.addMethod)
        self.methodsMenu.addAction(self.deleteMethod)

        self.methodsMenu.popup(QCursor.pos())

    def createMethod(self):
        self.createNewMethod.emit(1)


# %% Quality control levels table view
# noinspection PyUnresolvedReferences
class QualityTable(GenericTableView):

    # signal to pass dictionary to databases dock-widget
    createNewQuality = pyqtSignal(object)

    # define default class instances
    qualityMenu = None
    addQuality = None
    deleteQuality = None

    def __init__(self, dictionary, header, col_alignment):
        super(QualityTable, self).__init__(dictionary, header, col_alignment)

        # table view
        self.tableView.customContextMenuRequested.connect(self.openMenu)
        self.tableView.verticalHeader().setDefaultSectionSize(18)

    # right click menu
    def openMenu(self):
        # drop menu [sites menu and timeseries menu]
        self.qualityMenu = QMenu('Quality')
        self.addQuality = QAction('Add quality (single/multiple)...', self)
        self.deleteQuality = QAction('Delete quality', self)

        self.addQuality.triggered.connect(self.createQuality)

        self.qualityMenu.addAction(self.addQuality)
        self.qualityMenu.addAction(self.deleteQuality)

        self.qualityMenu.popup(QCursor.pos())

    def createQuality(self):
        self.createNewQuality.emit(1)


# %% Qualifiers table view
# noinspection PyUnresolvedReferences
class QualifiersTable(GenericTableView):

    # signal to pass dictionary to databases dock-widget
    createNewQualifier = pyqtSignal(object)

    # define default class instances
    qualifiersMenu = None
    addQualifier = None
    deleteQualifier = None

    def __init__(self, dictionary, header, col_alignment):
        super(QualifiersTable, self).__init__(dictionary, header, col_alignment)

        # table view
        self.tableView.customContextMenuRequested.connect(self.openMenu)
        self.tableView.verticalHeader().setDefaultSectionSize(18)

    # right click menu
    def openMenu(self):
        # drop menu [sites menu and timeseries menu]
        self.qualifiersMenu = QMenu('Qualifiers')
        self.addQualifier = QAction('Add qualifier (single/multiple)...', self)
        self.deleteQualifier = QAction('Delete qualifier', self)

        self.addQualifier.triggered.connect(self.createQualifier)

        self.qualifiersMenu.addAction(self.addQualifier)
        self.qualifiersMenu.addAction(self.deleteQualifier)

        self.qualifiersMenu.popup(QCursor.pos())

    def createQualifier(self):
        self.createNewQualifier.emit(1)


# %% Tab-widget to show database structure
class DbTabView(QTabWidget):
    """
    Create table-widget to display in Hydro-ClimaT
    """
    def __init__(self, engine, parent=None):
        super(DbTabView, self).__init__(parent)

        self.sourcesView = QWidget()
        self.sitesView = QWidget()
        self.variablesView = QWidget()
        self.methodsView = QWidget()
        self.qualitiesView = QWidget()
        self.qualifiersView = QWidget()
        self.metadataView = QWidget()

        # get dictionaries
        self.sourcesDict = SqlQuery.get_sources_table(engine)
        self.sitesDict = SqlQuery.get_sites_table(engine)
        self.variablesDict = SqlQuery.get_vars_table(engine)
        self.methodsDict = SqlQuery.get_methods_table(engine)
        self.qualitiesDict = SqlQuery.get_qualities_table(engine)
        self.qualifiersDict = SqlQuery.get_qualifiers_table(engine)
        self.metadataDict = SqlQuery.get_metadata_table(engine)

        # transform dictionaries to be uniform
        siteCodes = list(self.sitesDict.keys())
        siteNames = [i[1][1] for i in self.sitesDict.items()]
        siteLons = [i[1][2] for i in self.sitesDict.items()]
        siteLats = [i[1][3] for i in self.sitesDict.items()]
        self.sitesDict = {'Code': siteCodes, 'Name': siteNames, 'Latitude': siteLats,
                          'Longitude': siteLons}

        # create table view layouts for each parameter
        self.sitesTable = SitesTable(self.sitesDict, ['Code', 'Name', 'Latitude', 'Longitude',
                                                      'Variables'], [2, 0, 2, 2, 2])
        self.sourcesTable = SourcesTable(self.sourcesDict,
                                         ['ID', 'Organization', 'Description', 'Metadata ID'],
                                         [1, 0, 0, 1])

        self.metadataTable = MetadataTable(self.metadataDict,
                                           ['ID', 'Topic Category', 'Title', 'Abstract', 'Link',
                                            'Profiler version'], [1, 0, 0, 0, 0, 0])

        self.variablesTable = VariablesTable(self.variablesDict,
                                             ['ID', 'Variable', 'Units', 'Time Resolution', 'Type',
                                              'No Data'], [1, 0, 0, 0, 0, 2])

        self.methodsTable = MethodsTable(self.methodsDict, ['ID', 'Description', 'Link'], [1, 0, 0])

        self.qualitiesTable = QualityTable(self.qualitiesDict,
                                           ['ID', 'Code', 'Definition', 'Explanation'], [1, 1, 0, 0])

        self.qualifiersTable = QualifiersTable(self.qualifiersDict, ['ID', 'Code', 'Description'],
                                               [1, 1, 0])

        # set table views layouts
        self.sitesView.setLayout(self.sitesTable)
        self.metadataView.setLayout(self.metadataTable)
        self.sourcesView.setLayout(self.sourcesTable)
        self.variablesView.setLayout(self.variablesTable)
        self.methodsView.setLayout(self.methodsTable)
        self.qualitiesView.setLayout(self.qualitiesTable)
        self.qualifiersView.setLayout(self.qualifiersTable)

        self.addTab(self.sitesView, 'Sites && Series')
        self.addTab(self.metadataView, 'Metadata')
        self.addTab(self.sourcesView, 'Sources')
        self.addTab(self.variablesView, 'Variables')
        self.addTab(self.methodsView, 'Methods')
        self.addTab(self.qualitiesView, 'Quality')
        self.addTab(self.qualifiersView, 'Qualifiers')

        self.setTabPosition(QTabWidget.TabPosition(1))


# %% Tab-widget to show analyzed timeseries
# noinspection PyUnresolvedReferences
class TimeseriesTreeView(QTreeWidget):
    """
    Create tree-view to display in Hydro-ClimaT
    """
    # signal to pass timeseries to timeseries dictionary in main-window
    workspaceTimeSeries = pyqtSignal(object)
    changeWorkspace = pyqtSignal(object)

    # define default class instances
    l1 = None
    l2 = None
    l3 = None
    item = None
    tsId = None
    menu = None
    deleteTimeSeries = None
    processTimeSeries = None

    def __init__(self, parent=None):
        super(TimeseriesTreeView, self).__init__(parent)

        self.setHeaderLabels(['Site', 'Method', 'Data quality', 'Id', 'SiteId', 'LongMethod'])
        self.setSortingEnabled(True)
        self.sortByColumn(0, Qt.AscendingOrder)
        self.setColumnWidth(0, 250)
        self.setColumnHidden(4, True)
        self.setColumnHidden(5, True)

        self.setContextMenuPolicy(Qt.CustomContextMenu)
        self.customContextMenuRequested.connect(self.openMenu)
        self.itemSelectionChanged.connect(self.emitChangeWorkspace)

    def addSite(self, ts_id, time_series_identifier):
        siteType = time_series_identifier[2]
        dataQuality = time_series_identifier[3]
        code = time_series_identifier[4][0]
        name = time_series_identifier[4][1]

        # tree widget structure
        source = [time_series_identifier[0]]
        variable = [time_series_identifier[1]]
        timeseries = [code + ' - ' + name, siteType[-2:], dataQuality, str(ts_id), code, siteType]
        self.l1 = QTreeWidgetItem(source)                   # level 1 [Source]
        self.l2 = QTreeWidgetItem(variable)                 # level 2 [Variable]
        self.l3 = QTreeWidgetItem(timeseries)               # level 3 [Timeseries]
        self.l3.setForeground(0, QBrush(QColor("grey")))
        self.l3.setForeground(1, QBrush(QColor("grey")))
        self.l3.setForeground(2, QBrush(QColor("grey")))
        self.l3.setForeground(3, QBrush(QColor("grey")))

        # establish that timeseries are not processed (displayed)
        self.l3.isDisplayed = False

        # get all tree-view items
        items = []
        # noinspection PyTypeChecker
        treeIterator = QTreeWidgetItemIterator(self, QTreeWidgetItemIterator.All)
        while treeIterator.value():
            if treeIterator.value().columnCount() == 1:
                value = treeIterator.value().text(0)
                items.append(value)
            else:
                value = [treeIterator.value().text(0), treeIterator.value().text(1),
                         treeIterator.value().text(2)]
                items.append(value)
            treeIterator += 1

        # add timeseries to tree-view
        root = self.invisibleRootItem()
        if source[0] in items:
            l1_child_count = root.childCount()
            for i in range(l1_child_count):
                l1_item = root.child(i)
                if l1_item.text(0) == source[0]:
                    if variable[0] not in items:
                        l1_item.addChild(self.l2)
                        self.l2.addChild(self.l3)
                        break
                    else:
                        l2_child_count = l1_item.childCount()
                        for j in range(l2_child_count):
                            l2_item = l1_item.child(j)
                            if l2_item.text(0) == variable[0]:
                                if timeseries[:-3] not in items:
                                    l2_item.addChild(self.l3)
                                    break
        else:
            self.l1.addChild(self.l2)
            self.l2.addChild(self.l3)
            self.addTopLevelItem(self.l1)

    def openMenu(self):
        self.item = self.currentItem()          # selected item
        if self.item.parent() and self.item.parent().parent():
            # check if item is a timeseries item
            self.tsId = self.item.text(3)   # timeseries id in dictionary
            self.menu = QMenu()
            self.deleteTimeSeries = QAction('Delete timeseries', self)
            self.processTimeSeries = QAction('Display timeseries', self)
            self.processTimeSeries.triggered.connect(self.emitTimeSeriesParameters)
            self.menu.addAction(self.processTimeSeries)
            self.menu.addSeparator()
            self.menu.addAction(self.deleteTimeSeries)
            self.menu.popup(QCursor.pos())

            if self.item.isDisplayed:
                self.processTimeSeries.setEnabled(False)

    def emitTimeSeriesParameters(self):
        # emit timeseries parameters to query in the workspace
        self.workspaceTimeSeries.emit(self.tsId)
        self.item.isDisplayed = True
        self.item.setForeground(0, QBrush(QColor("green")))
        self.item.setForeground(1, QBrush(QColor("green")))
        self.item.setForeground(2, QBrush(QColor("green")))
        self.item.setForeground(3, QBrush(QColor("green")))

    def emitChangeWorkspace(self):
        self.item = self.currentItem()          # selected item
        if self.item.parent():                  # check if item is a timeseries item
            if self.item.parent().parent():
                self.tsId = self.item.text(3)   # timeseries id in dictionary
                self.changeWorkspace.emit(self.tsId)

# %% Vector timeseries view

# %% Matrix timeseries view

# %% Monthly report view

# %% Missing analysis report view
