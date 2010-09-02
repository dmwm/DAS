
import logging

class PipeHandler(logging.Handler):
    "LogHandler to redirect records to the web pipe."
    def __init__(self, pipe):
        self.pipe = pipe
        logging.Handler.__init__(self, 1)
    def emit(self, record):
        self.pipe.send(('log', record))
        
def elem(what, inner='', **kwargs):
    "Return an HTML element with given text and attributes."
    argstr = ' '.join([what]+["%s='%s'" % (k, v) 
                              for k,v in kwargs.items()])
    return '<%s>%s</%s>' % (argstr, inner, what)