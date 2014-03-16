#!/usr/bin/env python
"""
Validation of inputvals json file (in mongoimport format).
"""
# __author__ = 'Vidmantas Zemleris'
import sys
from DAS.tools.schema_validators.json_validator import validate_mongodb_json
from DAS.tools.schema_validators.schema import Schema, And, Or

INPUTVALS_SCHEMA = \
    Schema({'ts': Or(float, int, long),
            'value': basestring})

def main():
    """Main function"""
    if  len(sys.argv) != 2:
        print "Usage: validator <inputvals_update_file.js>"
        sys.exit(1)
    validate_mongodb_json(INPUTVALS_SCHEMA, sys.argv[1])
#
# main
#
if __name__ == '__main__':
    main()
