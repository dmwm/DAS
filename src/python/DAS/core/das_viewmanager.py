#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
View manager class.
"""

__revision__ = "$Id: das_viewmanager.py,v 1.4 2009/07/14 15:58:46 valya Exp $"
__version__ = "$Revision: 1.4 $"
__author__ = "Valentin Kuznetsov"

import os

from sqlalchemy import Column, Integer, String, Text, DateTime
from sqlalchemy import create_engine, MetaData, ForeignKey, and_
from sqlalchemy.orm import relation, backref
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm.exc import MultipleResultsFound

# DAS modules
from DAS.utils.utils import genkey, getarg, sort_data

Base = declarative_base()
class Group(Base):
    """Group ORM"""
    __tablename__  = 'groups'
    __table_args__ = {'mysql_engine':'InnoDB'}

    id = Column(Integer, primary_key=True)
    name = Column(String(10), nullable=False, unique=True)

    def __init__(self, name):
        self.name = name

    def __repr__(self):
        """String representation of Group ORM object"""
        return "<Group('%s')>" % self.name

class User(Base):
    """User ORM"""
    __tablename__  = 'users'
    __table_args__ = {'mysql_engine':'InnoDB'}

    id          = Column(Integer, primary_key=True)
    login       = Column(Text, nullable=False, unique=True)
    fullname    = Column(Text, nullable=False, unique=True)
    contact     = Column(Text, nullable=True, unique=True)
    group_id    = Column(Integer, ForeignKey('groups.id'))
    group       = relation(Group, backref=backref('groups', order_by=id))

    def __init__(self, login, name, contact):
        self.login    = login
        self.fullname = name
        self.contact  = contact

    def __repr__(self):
        """String representation of User ORM object"""
        return "<User('%s', '%s', '%s')>" % \
                (self.login, self.fullname, self.contact)

class View(Base):
    """View ORM"""
    __tablename__  = 'views'
    __table_args__ = {'mysql_engine':'InnoDB'}

    id         = Column(Integer, primary_key=True)
    hash       = Column(String(32), unique=True)
    name       = Column(Text)
    definition = Column(Text)
    datetime   = Column(DateTime(timezone='CEST'))
    user_id    = Column(Integer, ForeignKey('users.id'))
    user       = relation(User, backref=backref('users', order_by=id))

    def __init__(self, ihash, iname, idef, idate):
        self.hash       = ihash
        self.name       = iname
        self.definitino = idef
        self.datetime   = idate

    def __repr__(self):
        """String representation of View ORM object"""
        return "<View('%s', '%s','%s', '%s')>" \
                % (self.hash, self.name, self.create, self.expire)

class DASViewManager(object):
    """
    View manager class responsible of mapping user defined
    views into DAS queries
    """
    def __init__(self, config):
        self.map = {} # should be replaced with persistent storage
        self.dir     = config['views_dir'] 
        if  not os.path.isdir(self.dir):
            os.mkdir(self.dir)
        dbengine     = config['views_engine'] 
        dbfile       = None
        if  dbengine.find('sqlite:///') != -1:
            dbfile   = dbengine.replace('sqlite:///', '')
        self.engine  = create_engine(dbengine, echo=False)
        self.session = sessionmaker(bind=self.engine)
        if  not dbfile:
            self.create_table()
        else: # sqlite case
            if  not os.path.isfile(dbfile):
                self.create_table()

    def create_table(self):
        """Create DB tables based on ORM objects"""
        metadata = MetaData()
        table    = View.__table__
        Base.metadata.create_all(self.engine)

    def create(self, name, query):
        """
        create new view for given name and a query
        """
        # TODO: query validation via DAS QL parser
        if  self.map.has_key(name):
            msg = "View '%s' already exists" % name
            raise Exception(msg)
        self.map[name] = query.strip().split(' where ')[0]

    def delete(self, name):
        """
        delete existing view
        """
        if  self.map.has_key(name):
            del(self.map[name])

    def update(self, name, query):
        """
        update exising view with new given query
        """
        if  self.map.has_key(name):
            # TODO: we can add logic to keep track of updates here
            self.delete(name)
            self.create(name, query)
        else:
            raise Exception("View '%s', doesn't exists" % name)
        

    def get(self, name):
        """
        retrieve DAS query for given name
        """
        if  not self.map.has_key(name):
            raise Exception("View '%s', doesn't exists" % name)
        return self.map[name]

    def all(self):
        """
        retrieve all views
        """
        return self.map

