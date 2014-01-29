"""
The modules provide schema validation, particularly validating
the JSON files in the mongoimport format.

In general each validator checks:
 * presence of mandatory fields
 * correct naming/typing of known fields

Note: schema.py is taken from https://github.com/halst/schema
"""
