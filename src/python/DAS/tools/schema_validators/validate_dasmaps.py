#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
# __author__ = 'Vidmantas Zemleris'
"""
Provide validation of DAS Maps

The validator checks:
 * presence of mandatory fields
 * correct naming/typing of known fields
 * validity of hash value, if one present
"""
import hashlib
import json
import sys

from DAS.tools.schema_validators.schema import Schema, And, Or, Optional
from DAS.tools.schema_validators.json_validator import validate_file_verbose


def check_hash(rec):
    """ Check record hash """
    nrec = dict(rec)
    if 'hash' in nrec:
        md5 = nrec.pop('hash')
        # exclude timestamps since they are dynamic
        if 'ts' in nrec:
            del nrec['ts']
        if 'timestamp' in nrec:
            del nrec['timestamp']
        nrec = json.JSONEncoder(sort_keys=True).encode(nrec)
        keyhash = hashlib.md5()
        keyhash.update(nrec)
        rmd5 = keyhash.hexdigest()
        if md5 != rmd5:
            print 'Invalid hash'
            print "record:", rec
            print "nrec:", nrec
            print "md5   :", md5
            print "rmd5  :", rmd5
            return False
    return True


_str = basestring  # a shorthand for string type

# schema of map_record
MAP_RECORD = And(
    {
        'hash': _str,
        'format': _str,
        'url': _str,
        'urn': _str,
        'ts': Or(float, int, long),
        'system': _str,
        'das_map': [{
                        'rec_key': _str,
                        'das_key': _str,
                        Optional('api_arg'): _str,
                        Optional('pattern'): _str}],
        'services': Or({_str: _str},
                       # it can also be empty value
                       lambda v: not v),
        'expire': int,
        'lookup': _str,
        'wild_card': _str,
        'params': Or({}, {_str: Or(_str, bool, list)}),
        'type': 'service',
        Optional('instances'): list,
    },
    check_hash)

# schema of presentation_map record
PRESENTATION_RECORD = And(
    {
    'presentation': dict,
    'ts': Or(float, int, long),
    'hash': _str,
    'type': 'presentation'},
    check_hash)

# schema of notation_map record
NOTATION_RECORD = And(
    {'notations': [{'rec_key': _str,
                    'api': _str,
                    'api_output': _str}],
     'system': _str,
     'ts': Or(float, int, long),
     'hash': _str,
     'type': 'notation'
     # TODO: allow optional str:str, as in original validator
     # Optional(_str): _str
     },
    check_hash)

# schema of "arecord"
ARECORD_RECORD = {'arecord': dict}

# a record defining how input values can be fetched from given service
INPUTVALUES_RECORD = And(
    {'input_values': [
        {'url': _str,
         'input': _str,
         'jsonpath_selector': _str,
         Optional('test'): _str}, ],
     'type': 'input_values',
     'system': _str,
     'hash': _str},
    check_hash)

# TODO: could even check if sum of tokens matches, but this is not important
VERIF_TOKEN_RECORD = {'verification_token': _str,
                      'type': 'verification_token'}

# define a list of complementary rules, where at least one must match, this
# instead of just Schema(Or(a, b, c)) will provide more informative feedback
SCHEMA_RULES = {
    'arecord_record': Schema(ARECORD_RECORD),
    'map_record': Schema(MAP_RECORD),
    'presentation_record': Schema(PRESENTATION_RECORD),
    'notation_record': Schema(NOTATION_RECORD),
    'inputvalues_record': Schema(INPUTVALUES_RECORD),
    'verification_token': Schema(VERIF_TOKEN_RECORD)}


def main():
    """Main function"""
    if len(sys.argv) != 2:
        print "Usage: validator <dasmap_update_file.js>"
        sys.exit(1)
    if not validate_file_verbose(SCHEMA_RULES, sys.argv[1]):
        sys.exit(1)

#
# main
#
if __name__ == '__main__':
    main()
