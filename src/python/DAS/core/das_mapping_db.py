#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DAS mapping DB
"""

from __future__ import with_statement

__revision__ = "$Id: das_mapping_db.py,v 1.2 2009/09/01 20:20:30 valya Exp $"
__version__ = "$Revision: 1.2 $"
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

class DASKey(Base):
    """DASKey ORM"""
    __tablename__ = 'daskeys'
    __table_args__ = {'mysql_engine':'InnoDB'}

    id   = Column(Integer, primary_key=True)
    name = Column(String(10), nullable=False, unique=True)

    def __init__(self, name):
        self.name = name

    def __repr__(self):
        """String representation of DASKey ORM object"""
        return "<DASKey('%s')>" % self.name

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

class APIMap(Base):
    """APIMap ORM"""
    __tablename__ = 'apimaps'
    __table_args__ = (
    UniqueConstraint('api_id', 'param', name='uix_1'),
    {'mysql_engine':'InnoDB'})

    id          = Column(Integer, primary_key=True)
    system_id   = Column(Integer, ForeignKey('systems.id'))
    api_id      = Column(Integer, ForeignKey('apis.id'))
    param       = Column(String(30), nullable=False)
    default     = Column(String(30), nullable=True)

    system      = relation(System, order_by=System.id)
    api         = relation(Api, order_by=Api.id)

    def __init__(self, param, default):
        self.param   = param
        self.default = default

    def __repr__(self):
        """String representation of APIMap ORM object"""
        return "<APIMap('%s', '%s, '%s', '%s')>" \
        % (self.system, self.api, self.param, self.default)

class DASMap(Base):
    """DASMap ORM"""
    __tablename__ = 'dasmaps'
    __table_args__ = (
    UniqueConstraint('api_id', 'daskey_id', name='uix_1'),
    {'mysql_engine':'InnoDB'})

    id          = Column(Integer, primary_key=True)
    system_id   = Column(Integer, ForeignKey('systems.id'))
    api_id      = Column(Integer, ForeignKey('apis.id'))
    daskey_id   = Column(Integer, ForeignKey('daskeys.id'))
    primary_key = Column(String(30), nullable=False)

    system      = relation(System, order_by=System.id)
    api         = relation(Api, order_by=Api.id)
    daskey      = relation(DASKey, order_by=DASKey.id)

    def __init__(self, primary_key):
        self.primary_key = primary_key

    def __repr__(self):
        """String representation of DASMap ORM object"""
        return "<DASMap('%s', '%s, '%s', '%s')>" \
        % (self.system, self.api, self.daskey, self.primary_key)

class API2DAS(Base):
    """API2DAS ORM"""
    __tablename__ = 'api2das'
    __table_args__ = (
    {'mysql_engine':'InnoDB'})

    id          = Column(Integer, primary_key=True)
    system_id   = Column(Integer, ForeignKey('systems.id'))
    api_id      = Column(Integer, ForeignKey('apis.id'))
#    param_id    = Column(Integer, ForeignKey('apimaps.param'))
    param_id    = Column(Integer, ForeignKey('apimaps.id'))
    daskey_id   = Column(Integer, ForeignKey('daskeys.id'))
    pattern     = Column(String(10), nullable=True)

    system      = relation(System, order_by=System.id)
    api         = relation(Api, order_by=Api.id)
    daskey      = relation(DASKey, order_by=DASKey.id)
#    param       = relation(APIMap, order_by=APIMap.param)
    param       = relation(APIMap, order_by=APIMap.id)

    def __init__(self, pattern):
        self.pattern = pattern

    def __repr__(self):
        """String representation of API2DAS ORM object"""
        return "<API2DAS('%s', '%s, '%s', '%s', '%s')>" \
        % (self.system, self.api, self.param, self.daskey, self.pattern)

class DASNotation(Base):
    """DASNotation ORM"""
    __tablename__ = 'dasnotations'
    __table_args__ = {'mysql_engine':'InnoDB'}

    id        = Column(Integer, primary_key=True)
    system_id = Column(Integer, ForeignKey('systems.id'))
    api_param = Column(String(10), nullable=False)
    das_param = Column(String(10), nullable=False)
#    api_id    = Column(Integer, ForeignKey('apis.id'))
    system    = relation(System, order_by=System.id)
#    api       = relation(Api, order_by=Api.id)

    def __init__(self, api_param, das_param):
        self.api_param = api_param
        self.das_param = das_param

    def __repr__(self):
        """String representation of DASNotation ORM object"""
#        return "<DASNotation('%s', '%s', '%s', '%s')>" \
#        % (self.system, self.api, self.api_param, self.das_param)
        return "<DASNotation('%s', '%s', '%s')>" \
        % (self.system, self.api_param, self.das_param)

class DASMappingMgr(object):
    """
    This class manages DAS mapping DB.
    """
    def __init__(self, config):
        self.logger  = config['logger']
        self.verbose = config['verbose']
        dbengine     = config['mapping_db_engine'] 
        if  self.verbose:
            self.logger.info("Init DAS Mapping DB %s" % dbengine)
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

    def add_api(self, system, api, args, daskeys, api2das):
        """
        Add API into mapping DB. Here args is a dict of API parameters;
        daskeys is a dict of DAS keys which this API will cover, it contains
        daskey:primary_key map;
        api2das is a list of triplets: (param, daskey, pattern)
        """
        aobj = Api(api)
        session = self.session()
        try:
            sobj = session.query(System).filter(System.name==system).one()
            aobj.system = sobj
            session.add(aobj)
            session.commit()
            # add API arguments
            for key, val in args.items():
                apimapobj = APIMap(key, val)
                apimapobj.api = aobj
                apimapobj.system = sobj
                session.add(apimapobj)
                session.commit()
            # add DAS keys
            for daskey, primkey in daskeys.items():
                try: # add new DAS key
                    kobj = session.query(DASKey).\
                        filter(DASKey.name==daskey).one()
                except:
                    kobj = DASKey(daskey)
                    session.add(kobj)
                    session.commit()
                    pass
                dasmap        = DASMap(primkey) 
                dasmap.api    = aobj
                dasmap.system = sobj
                dasmap.daskey = kobj
                session.add(dasmap)
                session.commit()
            # loop over list of triplets to fill API2DAS table
            for triplet in api2das:
                param = triplet[0]
                daskey = triplet[1]
                pattern = triplet[2]
                try:
                    kobj = session.query(DASKey).\
                        filter(DASKey.name==daskey).one()
                except:
                    kobj = DASKey(daskey)
                    session.add(kobj)
                    session.commit()
                    pass
                pobj = session.query(APIMap).filter(APIMap.param==param).\
                        filter(APIMap.api_id==aobj.id).one()

                api2dasobj = API2DAS(pattern)
                api2dasobj.system = sobj
                api2dasobj.api = aobj
                api2dasobj.param = pobj
                api2dasobj.daskey = kobj
                session.add(api2dasobj)
                session.commit()

            session.commit()
        except: 
            traceback.print_exc()
            session.rollback()

    def add_system(self, system):
        """
        Add new sub-system into mapping DB
        """
        obj = System(system)
        session = self.session()
        try:
            session.add(obj)
            session.commit()
        except: 
            session.rollback()

#    def add_notation(self, system, api, param, new_param):
    def add_notation(self, system, param, new_param):
        """
        Add new notation mapping into mapping DB
        """
        obj = DASNotation(param, new_param)
        session = self.session()
        try:
            sobj = session.query(System).filter(System.name==system).one()
#            try:
#                aobj = session.query(Api).filter(Api.name==api).one()
#            except:
#                aobj = System(system)
#                session.add(aobj)
#                pass
            obj.system = sobj
#            obj.api = aobj
            session.add(obj)
            session.commit()
        except: 
            session.rollback()
            traceback.print_exc()

    def add_daskey(self, daskey):
        """
        Add new DAS key
        """
        session = self.session()
        dobj = DASKey(daskey)
        session.add(dobj)
        session.commit()

    def delete_daskey(self, key):
        """
        Delete new DAS key in mapping DB
        """
        session = self.session()
        try: # transactions
            kobj = session.query(DASKey).filter(DASKey.name==key).one()
            for mobj in session.query(DASMap).filter(DASMap.daskey_id==kobj.id):
                session.delete(mobj)
            session.delete(kobj)
            session.commit()
        except:
            session.rollback()
            return False
        return True

    def delete_api(self, api):
        """
        Delete new api w/ parameters in mapping DB
        """
        session = self.session()
        try: # transactions
            aobj = session.query(Api).filter(Api.name==api).one()
            session.delete(aobj)
            res = session.query(APIMap).filter(APIMap.api_id==aobj.id)
            for row in res:
                session.delete(row)
            res = session.query(API2DAS).filter(API2DAS.api_id==aobj.id)
            for row in res:
                session.delete(row)
            res = session.query(DASMap).filter(DASMap.api_id==aobj.id)
            for row in res:
                session.delete(row)
            session.commit()
        except:
            session.rollback()
            return False
        return True

    def delete_system(self, system):
        """
        Delete new sub-system in mapping DB
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

    def delete_notation(self, system, param):
        """
        Delete new notation mapping into mapping DB
        """
        session = self.session()
        try: # transactions
            sobj = session.query(System).filter(System.name==system).one()
            nobj = session.query(DASNotation).\
                filter(DASNotation.system_id==sobj.id).\
                filter(DASNotation.api_param==param).one()
            session.delete(nobj)
            session.commit()
        except:
            session.rollback()
            return False
        return True

    def list_notations(self, system=None):
        """
        List all notations from mapping DB
        """
        session = self.session()
        try:
            res = session.query(DASNotation, System).\
                filter(DASNotation.system_id==System.id)
            if  system:
                res = res.filter(System.name==system)
            session.commit()
        except: 
            session.rollback()
            return []
        return [(sobj.name, nobj.name) for nobj, sobj in res]

    def list_systems(self):
        """
        List all DAS keys from mapping DB
        """
        session = self.session()
        try:
            res = session.query(System)
            session.commit()
        except: 
            session.rollback()
            return []
        return [obj.name for obj in res]

    def list_daskeys(self, system=None):
        """
        List all DAS keys from mapping DB
        """
        session = self.session()
        try:
            res = session.query(DASKey, System, DASMap).\
                filter(DASMap.system_id==System.id).\
                filter(DASMap.daskey_id==DASKey.id)
            if  system:
                res = res.filter(System.name==system)
            session.commit()
        except: 
            session.rollback()
            return []
        keys = []
        for kobj, sobj, dmap in res:
            if  kobj.name not in keys:
                keys.append(kobj.name)
        return keys
#        return [k.name for k, s, o in res]

    def list_apis(self, system):
        """
        List all APIs from mapping DB
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
        Retrieve API parameters from mapping DB
        """
        params = {}
        session = self.session()
        try:
            res = session.query(Api, APIMap).filter(Api.id==APIMap.api_id).\
                filter(Api.name==api)
            session.commit()
        except: 
            session.rollback()
            return params
        for aobj, mobj in res:
            params[mobj.param] = mobj.default
        return params

    def api_keys(self, api):
        """
        Retrieve DAS keys for given API from mapping DB
        """
        session = self.session()
        try:
            res = session.query(Api, DASKey, DASMap).\
                filter(DASMap.api_id==Api.id).\
                filter(DASMap.daskey_id==DASKey.id).\
                filter(Api.name==api)
            session.commit()
        except: 
            session.rollback()
            return []
        keys = []
        for key in (kobj.name for aobj, kobj, mobj in res):
            if  key not in keys:
                keys.append(key)
        return keys

