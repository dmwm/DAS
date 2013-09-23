__author__ = 'vidma'

try:
    from DAS.keywordsearch.metadata.schema_adapter2 import DasSchemaAdapter
    __schema = DasSchemaAdapter

except ImportError:
    __schema = None




def getSchema(*args, **kwargs):
    """
    returns a concrete schema implementation, so keyword search can be
    more or less DAS independent

    args: dascore
    """
    global __schema
    return __schema(*args, **kwargs)

#getSchema = lambda *args, **kwargs: __schema(*args, **kwargs)


def setSchemaAdapter(schemaAdapter):
    """
    set a concrete schema adapter
    """
    global __schema
    __schema = schemaAdapter