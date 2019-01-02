#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from odm2api.ODMconnection import dbconnection

# Supporting Python3
try:
    import urllib.request as request
except ImportError:
    import urllib as request
import xml.etree.ElementTree as ET
import argparse

# ======================================================================================================================
# CV  Objects
# ======================================================================================================================
from sqlalchemy import Column,  String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class CVElevationDatum(Base):
    __tablename__ = 'cv_elevationdatum'
    __table_args__ = {u'schema': 'odm2'}

    Term = Column('term', String(255), nullable=False)
    Name = Column('name', String(255), primary_key=True)
    Definition = Column('definition', String(1000))
    Category = Column('category', String(255))
    SourceVocabularyUri = Column('sourcevocabularyuri', String(255))

    def __repr__(self):
        return "<CV('%s', '%s', '%s', '%s')>" % (self.Term, self.Name, self.Definition, self.Category)


