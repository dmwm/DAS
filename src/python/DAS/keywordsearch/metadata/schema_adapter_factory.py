__author__ = 'vidma'



__schema = None
def getSchema(*args, **kwargs):
    """
    returns a concrete schema implementation, so keyword search can be
    more or less DAS independent
    """
    global __schema
    # if not provided, default is DAS
    if not __schema:
        from DAS.keywordsearch.metadata.schema_adapter2 import DasSchemaAdapter
        __schema = DasSchemaAdapter
    return __schema(*args, **kwargs)

def setSchemaAdapter(schemaAdapter):
    """
    set a concrete schema adapter
    """
    global __schema
    __schema = schemaAdapter