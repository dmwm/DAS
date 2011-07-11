#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
Set of useful utilities used by DAS web applications
"""

__revision__ = "$Id: utils.py,v 1.21 2010/04/30 16:41:18 valya Exp $"
__version__ = "$Revision: 1.21 $"
__author__ = "Valentin Kuznetsov"

from   types import NoneType
import cgi
import time
import urllib
import hashlib
import cherrypy
import plistlib
from   cherrypy import HTTPError
from   json import JSONEncoder
from   urllib import quote_plus
from   pymongo.objectid import ObjectId

# DAS modules
import DAS.utils.jsonwrapper as json
from   DAS.utils.utils import print_exc
from   DAS.utils.regex import number_pattern, web_arg_pattern
from   DAS.utils.das_db import db_connection
from   DAS.web.das_codes import web_code

# regex patterns used in free_text_parser
from DAS.utils.regex import PAT_BLOCK, PAT_RUN, PAT_FILE, PAT_RELEASE
from DAS.utils.regex import PAT_SITE, PAT_SE, PAT_DATATYPE, PAT_TIERS

def free_text_parser(sentence, daskeys, default_key="dataset"):
    """Parse sentence and construct DAS QL expresion"""
    found = 0
    for word in sentence.split():
        if  word.find('=') != -1 or word in daskeys:
            found = 1
    dasquery = ''
    if  not found:
        for word in sentence.split():
            if  PAT_RUN.match(word):
                dasquery += 'run=%s ' % word
            elif PAT_BLOCK.match(word):
                dasquery += 'block=%s ' % word
            elif PAT_FILE.match(word):
                dasquery += 'file=%s ' % word
            elif PAT_RELEASE.match(word):
                if  word.find('CMSSW_') == -1:
                    word = 'CMSSW_' + word
                if  word.find('*') != -1:
                    dasquery += 'release=%s ' % word
                else:
                    dasquery += 'release=%s* ' % word
            elif PAT_SITE.match(word):
                if  word.find('*') != -1:
                    dasquery += 'site=%s ' % word
                else:
                    dasquery += 'site=%s* ' % word
            elif PAT_SE.match(word):
                dasquery += 'site=%s ' % word
            elif PAT_TIERS.match(word):
                if  word.find('*') != -1:
                    dasquery += 'tier=%s ' % word
                else:
                    dasquery += 'tier=*%s* ' % word
            elif PAT_DATATYPE.match(word):
                dasquery += 'datatype=%s ' % word
            else:
                if  word.find('*') != -1:
                    dasquery += 'dataset=%s ' % word
                elif len(word) > 0 and word[0] == '/':
                    dasquery += 'dataset=%s* ' % word
                else:
                    dasquery += 'dataset=*%s* ' % word
    if  dasquery:
        if  dasquery.find(default_key) != -1:
            select_key = default_key
        else:
            select_key = dasquery.split('=')[0]
        dasquery = select_key + ' ' + dasquery
        return dasquery.strip()
    return sentence

def choose_select_key(dasinput, daskeys, default='dataset'):
    """
    For given das input query pick-up select key. If default key is found
    in input use, otherwise select key is a first DAS key
    """
    first_word = dasinput.split('|')[0].split()[0]
    if  first_word.find('=') != -1:
        if  dasinput.find('%s=' % default) != -1:
            return default
        first_word = first_word.split('=')[0]
    if  first_word in daskeys:
        return first_word
    return None
        
def gen_color(system):
    """
    Generate color for a system, use hash function for that
    """
    keyhash = hashlib.md5()
    keyhash.update(system)
    return '#%s' % keyhash.hexdigest()[:6]

def yui_name(name):
    """
    Parse input name and return compatible with YUI library name, e.g.
    YUI does not accepts period on a name in DataSource table objects.
    """
    return quote(str(name.replace('.', '__')))

def yui2das(name):
    """
    Reverse of yui_name.
    """
    return quote(str(name.replace('__', '.')))

def db_monitor(uri, func, sleep=5):
    """
    Check status of MongoDB connection. Invoke provided function upon 
    successfull connection.
    """
    conn = db_connection(uri)
    while True:
        time.sleep(sleep)
        if  not conn:
            conn = db_connection(uri)
            try:
                func()
            except:
                pass
            print "\n### re-established connection %s" % conn

def dascore_monitor(cdict, func, sleep=5):
    """
    Check status of DASCore and MongoDB connection for provided
    in cdict das/uri parameters. Invoke provided function upon 
    successfull connection.
    """
    uri  = cdict['uri']
    das  = cdict['das']
    conn = db_connection(uri)
    while True:
        time.sleep(sleep)
        if  not conn or not das:
            conn = db_connection(uri)
            try:
                if  conn['mapping']['db'].count():
                    time.sleep(3) # sleep to ensure that all maps are loaded
                    func() # re-initialize DAS
                    das = True # we do see maps in MongoDB
                else:
                    das = False
            except:
                das = False
            print "\n### re-established connection %s, mapping.db records %s" \
                % (conn, das)

def quote(data):
    """
    Sanitize the data using cgi.escape.
    """
    if  isinstance(data, int) or isinstance(data, float):
        res = data
    elif  isinstance(data, dict):
        res = data
    elif  isinstance(data, list):
        res = data
    elif  isinstance(data, long) or isinstance(data, int) or\
          isinstance(data, float):
        res = data
    elif  isinstance(data, ObjectId):
        res = str(data)
    else:
        try:
            if  data:
                res = cgi.escape(data, quote=True)
            else:
                res = ""
        except Exception as exc:
            print_exc(exc)
            print "Unable to cgi.escape(%s, quote=True)" % data
            res = ""
    return res

def get_ecode(error):
    """
    Process input HTTPError and extract DAS error code. If not found
    return back the error.
    """
    for line in error.split('\n'):
        if  line.find('DAS error, code') != -1:
            return line.strip().replace('<p>', '').replace('</p>', '')
    return error

def checkarg(kwds, arg, atype=str):
    """Check arg in a dict that it has provided type"""
    return kwds.has_key(arg) and isinstance(kwds[arg], atype)

def checkargs(supported):
    """
    Decorator to check arguments in provided supported list for DAS servers
    """
    def wrap(func):
        """Wrap input function"""
        def wrapped_f(self, *args, **kwds):
            """Wrap function arguments"""
            # check request headers. For methods POST/PUT
            # we need to read request body to get parameters
            headers = cherrypy.request.headers
            if  cherrypy.request.method == 'POST' or\
                cherrypy.request.method == 'PUT':
                try:
                    body = cherrypy.request.body.read()
                except:
                    body = None
                if  args and kwds:
                    code = web_code('Misleading request')
                    raise HTTPError(500, 'DAS error, code=%s' % code)
                if  body:
                    jsondict = json.loads(body, encoding='latin-1')
                else:
                    jsondict = kwds
                for key, val in jsondict.items():
                    kwds[str(key)] = str(val)

            pat = web_arg_pattern
            if  not kwds:
                if  args:
                    kwds = args[-1]
            keys = []
            if  not isinstance(kwds, dict):
                code  = web_code('Unsupported kwds')
                raise HTTPError(500, 'DAS error, code=%s' % code)
            if  kwds:
                keys = [i for i in kwds.keys() if i not in supported]
            if  keys:
                code  = web_code('Unsupported key')
                raise HTTPError(500, 'DAS error, code=%s' % code)
            if  checkarg(kwds, 'query') and not len(kwds['query']):
                code  = web_code('Invalid query')
                raise HTTPError(500, 'DAS error, code=%s' % code)
            if  checkarg(kwds, 'expire') and not pat.match(str(kwds['expire'])):
                code  = web_code('Unsupported expire value')
                raise HTTPError(500, 'DAS error, code=%s' % code)
            if  checkarg(kwds, 'order'):
                if  kwds['order'] not in ['asc', 'desc']:
                    code  = web_code('Unsupported order value')
                    raise HTTPError(500, 'DAS error, code=%s' % code)
            if  checkarg(kwds, 'skey'):
                if  not (isinstance(kwds['skey'], str) or \
                    isinstance(kwds['skey'], unicode)):
                    code  = web_code('Unsupported skey value')
                    raise HTTPError(500, 'DAS error, code=%s' % code)
            if  kwds.has_key('input'):
                if  not (isinstance(kwds['input'], str) or \
                    isinstance(kwds['input'], unicode)):
                    code  = web_code('Invalid input')
                    raise HTTPError(500, 'DAS error, code=%s' % code)
            if  checkarg(kwds, 'idx') and not pat.match(str(kwds['idx'])):
                code  = web_code('Unsupported idx value')
                raise HTTPError(500, 'DAS error, code=%s' % code)
            if  checkarg(kwds, 'limit') and not pat.match(str(kwds['limit'])):
                code  = web_code('Unsupported limit value')
                raise HTTPError(500, 'DAS error, code=%s' % code)
            if  checkarg(kwds, 'method'):
                if  kwds['method'] not in ['GET', 'PUT', 'DELETE', 'POST']:
                    code  = web_code('Unsupported method')
                    raise HTTPError(500, 'DAS error, code=%s' % code)
            if  checkarg(kwds, 'dir'):
                if  kwds['dir'] not in ['asc', 'desc']:
                    code  = web_code('Unsupported dir value')
                    raise HTTPError(500, 'DAS error, code=%s' % code)
            if  checkarg(kwds, 'sort'):
                if  kwds['sort'] not in ['true', 'false']:
                    code  = web_code('Unsupported sort value')
                    raise HTTPError(500, 'DAS error, code=%s' % code)
            if  checkarg(kwds, 'view'):
                if  kwds['view'] not in \
                        ['list', 'xml', 'json', 'plain', 'table']:
                    code  = web_code('Unsupported view')
                    raise HTTPError(500, 'DAS error, code=%s' % code)
            if  checkarg(kwds, 'format'):
                if  kwds['format'] not in ['xml', 'json']:
                    code  = web_code('Unsupported format')
                    raise HTTPError(500, 'DAS error, code=%s' % code)
            if  checkarg(kwds, 'database') and \
                not isinstance(kwds['database'], str):
                if  kwds['database'] not in \
                    ['das', 'analytics', 'admin', 'logging', 'mapping']:
                    code  = web_code('Unsupported database')
                    raise HTTPError(500, 'DAS error, code=%s' % code)
            if  checkarg(kwds, 'collection'):
                if  kwds['collection'] not in ['cache', 'merge']:
                    code  = web_code('Unsupported collection')
                    raise HTTPError(500, 'DAS error, code=%s' % code)
            if  checkarg(kwds, 'ajax'):
                if  str(kwds['ajax']) not in ['0', '1']:
                    code  = web_code('Unsupported ajax value')
                    raise HTTPError(500, 'DAS error, code=%s' % code)
            if  checkarg(kwds, 'dasquery') and not len(kwds['dasquery']):
                code = web_code('Unsupported dasquery value')
                raise HTTPError(500, 'DAS error, code=%s' % code)
            if  checkarg(kwds, 'dbcoll'):
                if  kwds['dbcoll'].find('.') == -1:
                    code = web_code('Unsupported dbcoll value')
                    raise HTTPError(500, 'DAS error, code=%s' % code)
            if  checkarg(kwds, 'msg') and not isinstance(kwds['msg'], str):
                code = web_code('Unsupported msg value')
                raise HTTPError(500, 'DAS error, code=%s' % code)
            if  checkarg(kwds, 'fid') and len(kwds['fid']) != 24:
                code = web_code('Unsupported id value')
                raise HTTPError(500, 'DAS error, code=%s' % code)
            if  checkarg(kwds, 'pid') and len(str(kwds['pid'])) != 32:
                code  = web_code('Unsupported pid value')
                raise HTTPError(500, 'DAS error, code=%s' % code)
            if  checkarg(kwds, 'instance'):
                if  kwds['instance'] not in \
                ['cms_dbs_prod_global', 'cms_dbs_caf_analysis_01',
                 'cms_dbs_ph_analysis_01', 'cms_dbs_ph_analysis_02']:
                    code  = web_code('Unsupported dbs instance')
                    raise HTTPError(500, 'DAS error, code=%s' % code)
            data = func (self, *args, **kwds)
            return data
        wrapped_f.__doc__  = func.__doc__
        wrapped_f.__name__ = func.__name__
        wrapped_f.exposed  = True
        return wrapped_f
    wrap.exposed = True
    return wrap

def not_to_link():
    """
    Return a list of primary keys which do not need to provide a web links
    on UI.
    """
    return ['config', 'lumi', 'group', 'jobsummary']

def json2html(idict, pad="", ref=None):
    """
    Convert input JSON into HTML code snippet. We sanitize values with
    quote function for HTML content (see in this module) and quote_plus
    (from urllib) for URL context.
    """
    def is_number(number):
        """Check if provided input is a number: int, long, float"""
        pat = number_pattern
        test = isinstance(number, int) or isinstance(number, float) or\
                isinstance(number, long) or pat.match(str(number))
        return test

    width = 100
    newline = '\n'
    orig_pad = pad
    sss = pad + '{' + newline
    for key, val in idict.items():
        if  key == 'das_id' or key == 'cache_id':
            if  isinstance(val, list):
                value = '['
                lpad = ' '*len(' "das_id": [')
                _width = len("'', ")
                for item in val:
                    _width += len(item)
                    if  _width > width:
                        value += '\n' + lpad
                        _width = len("'', ")
                    value += \
                        "<a href=\"/das/records/%s?collection=cache\">%s</a>, "\
                        % (quote_plus(item), quote(item))
                value = value[:-2] + ']'
            else:
                value = "<a href=\"/das/records/%s?collection=cache\">%s</a>"\
                        % (quote_plus(val), quote(val))
                if  len(str(val)) < 3: # aggregator's ids
                    value = val
            # we don't need to quote value here since 
            # it constructs sanitized URLs, see block above
            sss += pad + """ <code class="key">"%s": </code>%s""" \
                % (quote(key), value)
        elif key == 'gridfs_id':
            value = "<a href=\"/das/gridfs/%s\">%s</das>" \
                % (quote_plus(val), quote(val))
        elif isinstance(val, list):
            if  len(val) == 1:
                nline = ''
            else:
                nline = newline
            sss += pad + """ <code class="key">"%s": </code>""" \
                        % quote(key)
            sss += '[' + nline
            pad += " "*3
            ppp  = pad
            if  not nline:
                ppp  = ''
            for idx in range(0, len(val)):
                item = val[idx]
                if  isinstance(item, dict):
                    sss += json2html(item, pad, ref=key)
                else:
                    if  isinstance(item, NoneType):
                        sss += """%s<code class="null">None</code>""" \
                                % quote(ppp)
                    elif  is_number(item):
                        sss += """%s<code class="number">%s</code>""" \
                                % (quote(ppp), quote(item))
                    else:
                        sss += """%s<code class="string">"%s"</code>""" \
                                % (quote(ppp), quote(item))
                if  idx < len(val) - 1:
                    sss += ',' + nline
            sss += ']'
            pad = orig_pad
        elif isinstance(val, dict):
            sss += pad + """ <code class="key">"%s"</code>: """ \
                        % quote(key)
            pad += ' '*3
            # don't account for 1st pad
            sss += json2html(val, pad, ref=key)[len(pad):]
            pad  = pad[:-3]
        else:
            sss += pad + """ <code class="key">"%s"</code>""" \
                        % quote(key)
            if  isinstance(val, NoneType):
                sss += """: <code class="null">None</code>"""
            elif ref and (key == 'name' or key == 'se' or key == 'site') and \
                (isinstance(val, str) or isinstance(val, unicode)):
                refkey = ref
                if  key == 'se' or key == 'site':
                    refkey = 'site'
                if  key == 'name':
                    if  ref == 'child' or ref == 'parent':
                        if  str(val).rfind('.root') != -1:
                            refkey = 'file'
                        else:
                            refkey = 'dataset'
                    else:
                        refkey = ref
                if  refkey not in not_to_link():
                    query = "%s=%s" % (refkey, val)
                    sss += """: <a href="/das/request?%s">%s</a>""" \
                            % (urllib.urlencode({'input':query}), quote(val))
                else:
                    sss += ": %s" % quote(val)
            elif key == 'run_number':
                run_number = int(val)
                query = "run=%s" % run_number
                sss += """: <a href="/das/request?%s">%s</a>""" \
                        % (urllib.urlencode({'input':query}), run_number)
            elif key == 'ip':
                query = "ip=%s" % val
                sss += """: <a href="/das/request?%s">%s</a>""" \
                        % (urllib.urlencode({'input':query}), val)
            elif is_number(val):
                sss += """: <code class="number">%s</code>""" % quote(val)
            else:
                sss += """: <code class="string">"%s"</code>""" % quote(val)
        if  key != idict.keys()[-1]:
            sss += ',' + newline
        else:
            sss += ""
    sss += newline + pad + '}'
    return sss

def web_time():
    """
    Return time in format record in Cherrypy log.
    """
    # 2010-01-04 10:40:53,850
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(time.time()))

def ajax_response_orig(msg, tag="response", element="object"):
    """AJAX response wrapper"""
    page  = """<ajax-response><response type="%s" id="%s">""" % (element, tag)
    page += msg
    page += "</response></ajax-response>"
    print page
    return page

def ajax_response(msg):
    """AJAX response wrapper"""
    page  = """<ajax-response>"""
    page += "<div>" + msg + "</div>"
    page += "</ajax-response>"
    return page

def wrap2dasjson(data):
    """DAS JSON wrapper"""
    encoder = JSONEncoder()
    cherrypy.response.headers['Content-Type'] = "text/json"
    try:
        jsondata = encoder.encode(data)
        return jsondata
    except:
        return dict(error="Failed to JSONtify obj '%s' type '%s'" \
            % (data, type(data)))

def wrap2dasxml(data):
    """DAS XML wrapper.
    Return data in XML plist format, 
    see http://docs.python.org/library/plistlib.html#module-plistlib
    """
    plist_str = plistlib.writePlistToString(data)
    cherrypy.response.headers['Content-Type'] = "application/xml"
    return plist_str

