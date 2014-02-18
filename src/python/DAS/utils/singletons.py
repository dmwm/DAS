#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable=R0903
"""
File: das_singleton.py
Author: Valentin Kuznetsov <vkuznet@gmail.com>
Description: singleton implementation
            http://stackoverflow.com/questions/6760685/creating-a-singleton-in-python
"""

class Singleton(type):
    """Implementation of Singleton class"""
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = \
                    super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

# Examples

#Python2
#class MyClass(BaseClass):
#    __metaclass__ = Singleton

#Python3
#class MyClass(BaseClass, metaclass=Singleton):
#    pass
