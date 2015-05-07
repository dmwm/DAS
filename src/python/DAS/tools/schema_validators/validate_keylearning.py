#!/usr/bin/env python
"""
Validation of keylearning json file (mongoimport format).
"""
from __future__ import print_function
# __author__ = 'Vidmantas Zemleris'
import sys
from DAS.tools.schema_validators.json_validator import validate_mongodb_json
from DAS.tools.schema_validators.schema import Schema, And, Or


KEYLEARNING_SCHEMA = \
    Schema(Or({'keys': [basestring, ],
               'members': [basestring, ],
               'system': basestring,
               'urn': basestring},
              {'member': basestring,
               'stems': [basestring, ]}))


def main():
    """Main function"""
    if  len(sys.argv) != 2:
        print("Usage: validator <keylearning_update_file.js>")
        sys.exit(1)
    validate_mongodb_json(KEYLEARNING_SCHEMA, sys.argv[1])
#
# main
#
if __name__ == '__main__':
    main()
