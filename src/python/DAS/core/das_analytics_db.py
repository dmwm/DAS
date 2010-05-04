#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DAS analytics DB
"""

from __future__ import with_statement

__revision__ = "$Id: das_analytics_db.py,v 1.1 2009/09/09 20:59:38 valya Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "Valentin Kuznetsov"

import os
import traceback

from sqlalchemy import Column, Integer, String
from sqlalchemy import create_engine, MetaData, ForeignKey
from sqlalchemy.orm import relation, backref
from sqlalchemy.schema import UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()
class System(Base):
    """System ORM"""
    __tablename__ = 'systems'
    __table_args__ = {'mysql_engine':'InnoDB'}

    id = Column(Integer, primary_key=True)
    name = Column(String(10), nullable=False, unique=True)

    def __init__(self, name):
        self.name = name

    def __repr__(self):
        """String representation of System ORM object"""
        return "<System('%s')>" % self.name

class Api(Base):
    """Api ORM"""
    __tablename__ = 'apis'
    __table_args__ = {'mysql_engine':'InnoDB'}

    id        = Column(Integer, primary_key=True)
    system_id = Column(Integer, ForeignKey('systems.id'))
    name      = Column(String(30), nullable=False, unique=True)
    system    = relation(System, backref=backref('systems', order_by=id))

    def __init__(self, name):
        self.name = name

    def __repr__(self):
        """String representation of Api ORM object"""
        return "<Api('%s', '%s')>" % (self.system, self.name)

class ApiParam(Base):
    """APIParam ORM"""
    __tablename__ = 'apiparams'
    __table_args__ = (
    UniqueConstraint('api_id', 'param', name='uix_1'),
    {'mysql_engine':'InnoDB'})

    id          = Column(Integer, primary_key=True)
    system_id   = Column(Integer, ForeignKey('systems.id'))
    api_id      = Column(Integer, ForeignKey('apis.id'))
    param       = Column(String(30), nullable=False)
    value       = Column(String(30), nullable=True)

    system      = relation(System, order_by=System.id)
    api         = relation(Api, order_by=Api.id)

    def __init__(self, param, value):
        self.param = param
        self.value = value

    def __repr__(self):
        """String representation of ApiParam ORM object"""
        return "<ApiParam('%s', '%s, '%s', '%s')>" \
        % (self.system, self.api, self.param, self.value)

class ApiCounter(Base):
    """ApiCounter ORM"""
    __tablename__ = 'apicounters'
    __table_args__ = (
    {'mysql_engine':'InnoDB'})

    id          = Column(Integer, primary_key=True)
    system_id   = Column(Integer, ForeignKey('systems.id'))
    api_id      = Column(Integer, ForeignKey('apis.id'))
    param_id    = Column(Integer, ForeignKey('apiparams.id'))
    counter     = Column(Integer, nullable=False)

    system      = relation(System, order_by=System.id)
    api         = relation(Api, order_by=Api.id)
    param       = relation(ApiParam, order_by=ApiParam.id)

    def __init__(self):
        self.counter = 1

    def __repr__(self):
        """String representation of ApiParam ORM object"""
        return "<ApiCounter('%s', '%s, '%s', '%s')>" \
        % (self.system, self.api, self.param, self.counter)

class DASAnalytics(object):
    """
    This class manages DAS analytics DB.
    """
    def __init__(self, config):
        self.logger  = config['logger']
        self.verbose = config['verbose']
        dbengine     = config['analytics_db_engine'] 
        if  self.verbose:
            self.logger.info("Init DAS Analytics DB %s" % dbengine)
            echo = True
        else:
            echo = False
        dbfile = None
        if  dbengine.find('sqlite:///') != -1:
            dbfile   = dbengine.replace('sqlite:///', '')
        self.engine  = create_engine(dbengine, echo=echo)
        self.session = sessionmaker(bind=self.engine)
        if  not dbfile:
            self.create_db()
        else: # sqlite case
            if  not os.path.isfile(dbfile):
                self.create_db()

    def create_db(self):
        """Create DB tables based on ORM objects"""
        metadata = MetaData()
        Base.metadata.create_all(self.engine)

    def add_api(self, system, api, args):
        """
        Add API into analytics DB. Here args is a dict of API parameters.
        """
        session = self.session()
        try:
            sobj = session.query(System).filter(System.name==system).one()
            try:
                aobj = session.query(Api).filter(Api.name==api).one()
            except:
                aobj = Api(api)
                aobj.system = sobj
                session.add(aobj)
                session.commit()
            # add API arguments
            for key, val in args.items():
                try:
                    pobj = session.query(ApiParam).\
                        filter(ApiParam.param==key).\
                        filter(ApiParam.value==val).one()
                except:
                    pobj = ApiParam(key, val)
                    pobj.api = aobj
                    pobj.system = sobj
                    session.add(pobj)
                    session.commit()
                try:
                    res = session.query(System, Api, ApiParam, ApiCounter).\
                    filter(System.name==system).\
                    filter(Api.system_id==System.id).\
                    filter(Api.name==api).\
                    filter(ApiParam.api_id==Api.id).\
                    filter(ApiParam.system_id==System.id).\
                    filter(ApiCounter.system_id==System.id).\
                    filter(ApiCounter.api_id==Api.id).\
                    filter(ApiCounter.param_id==pobj.id).\
                    filter(ApiParam.param==key).\
                    filter(ApiParam.value==val).\
                    one()
                    cobj = res[-1]
                    cobj.counter = cobj.counter + 1
                except:
                    cobj = ApiCounter()
                    cobj.system = sobj
                    cobj.api = aobj
                    cobj.param = pobj
                session.add(cobj)
                session.commit()
            session.commit()
        except: 
            traceback.print_exc()
            session.rollback()

    def add_system(self, system):
        """
        Add new sub-system into analytics DB
        """
        obj = System(system)
        session = self.session()
        try:
            session.add(obj)
            session.commit()
        except: 
            session.rollback()

    def delete_api(self, api):
        """
        Delete new api w/ parameters in analytics DB
        """
        session = self.session()
        try: # transactions
            aobj = session.query(Api).filter(Api.name==api).one()
            session.delete(aobj)
            res = session.query(ApiParam).filter(ApiParam.api_id==aobj.id)
            for row in res:
                session.delete(row)
            res = session.query(ApiCounter).filter(ApiCounter.api_id==aobj.id)
            for row in res:
                session.delete(row)
            session.commit()
        except:
            session.rollback()
            return False
        return True

    def delete_system(self, system):
        """
        Delete new sub-system in analytics DB
        """
        session = self.session()
        try: # transactions
            res = session.query(System).filter(System.name==system).one()
            session.delete(res)
            session.commit()
        except:
            session.rollback()
            return False
        return True

    def list_systems(self):
        """
        List all DAS keys from analytics DB
        """
        session = self.session()
        try:
            res = session.query(System)
            session.commit()
        except: 
            session.rollback()
            return []
        return [obj.name for obj in res]

    def list_apis(self, system):
        """
        List all APIs from analytics DB
        """
        session = self.session()
        try:
            res = session.query(Api, System).filter(Api.system_id==System.id)
            if  system:
                res = res.filter(System.name==system)
            session.commit()
        except: 
            session.rollback()
            return []
        return [aobj.name for aobj, sobj in res]

    def api_params(self, api):
        """
        Retrieve API parameters from analytics DB
        """
        params = {}
        session = self.session()
        try:
            res = session.query(Api, ApiParam).filter(Api.id==ApiParam.api_id).\
                filter(Api.name==api)
            session.commit()
        except: 
            session.rollback()
            return params
        for aobj, mobj in res:
            params[mobj.param] = mobj.value
        return params

    def api_counter(self, api, args={}):
        """
        Retrieve API counter from analytics DB. User must supply
        API name and optional dict of parameters.
        """
        params = {}
        session = self.session()
        try:
            query = session.query(Api, ApiParam, ApiCounter).\
                filter(Api.id==ApiParam.api_id).\
                filter(Api.name==api).\
                filter(Api.id==ApiCounter.api_id).\
                filter(ApiParam.id==ApiCounter.param_id)
            for key, val in args.items():
                query = query.filter(ApiParam.param==key).\
                filter(ApiParam.value==val)
            res = query.all()
        except: 
            return 0
        counter = 0
        for aobj, pobj, cobj in res:
            param_count = cobj.counter
            param_name  = pobj.param
            param_value = pobj.value
            if  args:
                for key, val in args.items():
                    if  param_name == key and param_value == val and \
                        param_count > counter:
                        counter = param_count
            else:
                if  param_count > counter:
                    counter = param_count
        return counter

