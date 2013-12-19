#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=R0903
"""
File: das_singleton.py
Author: Valentin Kuznetsov <vkuznet@gmail.com>
Description: singleton implementation
            http://stackoverflow.com/questions/6760685/creating-a-singleton-in-python
"""

class Singleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]
