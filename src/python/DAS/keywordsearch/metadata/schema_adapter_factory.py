# coding=utf-8
"""
An abstraction layer to retrieve the schema adapter
"""
__all__ = ['get_schema']

SCHEMA_ADAPTER = None
try:
    from DAS.keywordsearch.metadata.schema_adapter2 import DasSchemaAdapter
    SCHEMA_ADAPTER = DasSchemaAdapter
except ImportError:
    pass


def get_schema(*args, **kwargs):
    """
    returns a concrete schema implementation, so keyword search can be
    more or less DAS independent

    args: dascore (optional)
    """
    return SCHEMA_ADAPTER(*args, **kwargs)
