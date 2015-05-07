#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
View manager class.
"""

__revision__ = "$Id: das_viewmanager.py,v 1.1 2010/01/19 19:02:57 valya Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "Valentin Kuznetsov"

# system modules
import os
import traceback
from datetime import datetime

# SQLAlchemy modules
from sqlalchemy import Column, Integer, String, Text, DateTime
from sqlalchemy import create_engine, MetaData, ForeignKey
from sqlalchemy.orm import relation, backref
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# DAS modules
from DAS.utils.utils import genkey

BASE = declarative_base()
class Group(BASE):
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

class User(BASE):
    """User ORM"""
    __tablename__  = 'users'
    __table_args__ = {'mysql_engine':'InnoDB'}

    id          = Column(Integer, primary_key=True)
    login       = Column(Text, nullable=False, unique=True)
    fullname    = Column(Text, nullable=False, unique=True)
    contact     = Column(Text, nullable=True)
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

class View(BASE):
    """View ORM"""
    __tablename__  = 'views'
    __table_args__ = {'mysql_engine':'InnoDB'}

    id         = Column(Integer, primary_key=True)
    hash       = Column(String(32), unique=True)
    name       = Column(Text, unique=True)
    definition = Column(Text)
    datetime   = Column(DateTime(timezone='CEST'))
    user_id    = Column(Integer, ForeignKey('users.id'))
    user       = relation(User, backref=backref('users', order_by=id))

    def __init__(self, ihash, iname, idef, idate):
        self.hash       = ihash
        self.name       = iname
        self.definition = idef
        self.datetime   = idate

    def __repr__(self):
        """String representation of View ORM object"""
        return "<View('%s', '%s','%s', '%s')>" \
                % (self.hash, self.name, self.definition, self.datetime)

def strip_query(query):
    """
    Strip and remove conditions from the query. To be used as a
    view definition
    """
    return query.strip().split(' where ')[0]

class DASViewManager(object):
    """
    View manager class responsible of mapping user defined
    views into DAS queries
    """
    def __init__(self, config):
        self.map = {} # should be replaced with persistent storage
        self.dir     = config['views_dir'] 
        self.logger  = config['logger']
        if  not os.path.isdir(self.dir):
            os.mkdir(self.dir)
        dbengine     = config['views_engine'] 
        dbfile       = None
        if  dbengine.find('sqlite:///') != -1:
            dbfile   = dbengine.replace('sqlite:///', '')
        self.engine  = create_engine(dbengine, echo=False)
        self.session = sessionmaker(bind=self.engine)
        if  not dbfile:
            self.create_db()
        else: # sqlite case
            if  not os.path.isfile(dbfile):
                self.create_db()
        if  'sum_views' in config:
            login = 'dascore'
            fullname = 'DAS'
            group = 'admin'
            existing_views = self.all()
            for viewname, viewdef in config['sum_views'].items():
                if  viewname not in existing_views:
                    self.create(viewname, viewdef, login, fullname, group)

    def create_db(self):
        """Create DB tables based on ORM objects"""
        metadata = MetaData()
        BASE.metadata.create_all(self.engine)

    def create(self, viewname, query, 
                login='nobody', fullname='N/A', group='users'):
        """
        create new view for given name and a query
        """
        if  viewname in self.map:
            msg = "View '%s' already exists" % viewname
            raise Exception(msg)
        squery   = strip_query(query)

        vobj     = View(genkey(squery), viewname, squery, datetime.today())
        session  = self.session()
        try: # session transactions
            try:
                uobj = session.query(User).filter(User.login==login).one()
            except:
                uobj = User(login, fullname, contact="")
                session.add(uobj)
                pass
            try:
                gobj = session.query(Group).filter(Group.name==group).one()
            except:
                gobj = Group(group)
                session.add(gobj)
                pass
            vobj.user  = uobj
            uobj.group = gobj
            session.add(vobj)
            session.commit()
        except:
            session.rollback()
            self.logger.debug(traceback.format_exc())
            traceback.print_exc()
            msg = "Unable to commit DAS view DB"
            raise Exception(msg)

    def delete(self, name):
        """
        delete existing view
        """
        session  = self.session()
        try: # session transactions
            try:
                view = session.query(View).filter(View.name==name).one()
                session.delete(view) # it is not cascade delete
            except:
                pass
            session.commit()
        except:
            session.rollback()
            self.logger.debug(traceback.format_exc())
            traceback.print_exc()
            msg = "Unable to delete DAS view DB"
            raise Exception(msg)

    def update(self, name, query):
        """
        update exising view with new given query
        """
        self.delete(name)
        self.create(name, query)

    def get(self, name):
        """
        retrieve DAS query for given name
        """
        session  = self.session()
        view = None
        try: # session transactions
            view = session.query(View).filter(View.name==name).one()
            session.commit()
        except:
            session.rollback()
            self.logger.debug(traceback.format_exc())
            msg = "Unable to look-up view '%s' in DB" % name
            raise Exception(msg)
        return view.definition

    def all(self):
        """
        retrieve all views
        """
        session  = self.session()
        views = None
        try: # session transactions
            views = session.query(View).all()
            session.commit()
        except:
            session.rollback()
            self.logger.debug(traceback.format_exc())
            msg = "Unable to look-up all views in DB"
            raise Exception(msg)
        viewdict = {}
        for view in views:
            viewdict[view.name] = view.definition
        return viewdict

