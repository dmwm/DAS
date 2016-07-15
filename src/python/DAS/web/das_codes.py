#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable=W0613,W0622,W0702

"""
DAS web status codes
"""

__author__ = "Valentin Kuznetsov"

DAS_WEB_CODES = [\
        (0  , 'N/A'),\
        (1  , 'Unsupported key'),\
        (2  , 'Unsupported value'),\
        (3  , 'Unsupported method'),\
        (4  , 'Unsupported collection'),\
        (5  , 'Unsupported database'),\
        (6  , 'Unsupported view'),\
        (7  , 'Unsupported format'),\
        (8  , 'Wrong type'),\
        (9  , 'Misleading request'),\
        (10 , 'Invalid query'),\
        (11 , 'Exception'),\
        (12 , 'Invalid input'),\
        (13 , 'Unsupported expire value'),\
        (14 , 'Unsupported order value'),\
        (15 , 'Unsupported skey value'),\
        (16 , 'Unsupported idx value'),\
        (17 , 'Unsupported limit value'),\
        (18 , 'Unsupported dir value'),\
        (19 , 'Unsupported sort value'),\
        (20 , 'Unsupported ajax value'),\
        (21 , 'Unsupported show value'),\
        (22 , 'Unsupported dasquery value'),\
        (23 , 'Unsupported dbcoll value'),\
        (24 , 'Unsupported msg value'),\
        (25 , 'Unable to start DASCore'),\
        (26 , 'No file id'),\
        (27 , 'Unsupported id value'),\
        (28 , 'Server error'),\
        (29 , 'Query is not suitable for this view'),\
        (30 , 'DAS parser error'),\
        (31 , 'Unsupported pid value'),\
        (32 , 'Unsupported interval value'),\
        (33 , 'Unsupported kwds'),\
        (34 , 'Unsupported dbs instance'),\
        (35 , 'Unsupported query cache value'),\
]
def decode_code(code):
    """Return human readable string for provided code ID"""
    for idx, msg in DAS_WEB_CODES:
        if  code == idx:
            return msg
    return 'N/A'

def web_code(error):
    """Return DAS WEB code for provided error string"""
    for idx, msg in DAS_WEB_CODES:
        if  msg.lower() == error.lower():
            return idx
    return -1
