#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=R0912,R0915,W0702,R0914
"""
Set of useful utilities used by DAS web applications
"""

__author__ = "Valentin Kuznetsov"

from   types import NoneType
import sys
import cgi
import time
import urllib
import hashlib
import cherrypy
import plistlib
from   cherrypy import HTTPError
from   json import JSONEncoder
from   urllib import quote_plus
from   bson.objectid import ObjectId

# DAS modules
import DAS.utils.jsonwrapper as json
from   DAS.utils.utils import print_exc, presentation_datetime
from   DAS.utils.regex import number_pattern, web_arg_pattern, http_pattern
from   DAS.utils.das_db import db_connection, is_db_alive
from   DAS.web.das_codes import web_code
from   DAS.utils.das_config import das_readconfig

# regex patterns used in free_text_parser
from DAS.utils.regex import PAT_BLOCK, PAT_RUN, PAT_FILE, PAT_RELEASE
from DAS.utils.regex import PAT_SITE, PAT_SE, PAT_DATATYPE, PAT_TIERS

class HtmlString(object):
    """
    a class which embeds a string to be displayed in html mode (quote() will not modify it).
    Precaution: all escaping has to be done before hand.
    """
    def __init__(self, str):
        self.str = str
    def __unicode__(self):
        return self.str
    __str__ = __unicode__


def set_no_cache_flags():
    "Set cherrypy flags to prevent caching"
    cherrypy.response.headers['Cache-Control'] = 'no-cache'
    cherrypy.response.headers['Pragma'] = 'no-cache'
    cherrypy.response.headers['Expires'] = 'Sat, 01 Dec 2001 00:00:00 GMT'

def set_cache_flags():
    "Set cherrypy flags to prevent caching"
    headers = cherrypy.response.headers
    for key in ['Cache-Control', 'Pragma']:
        if  headers.has_key(key):
            del headers[key]

def threshold(sitedbmgr, thr, config):
    "Return query threshold for cache clients"
    # NOTE: once DAS authentication will be in place
    # the group/roles will be provided in HTTP headers
    # (authn/authz values) and most of this code will go away
    try:
        user_dn = None
        for key, val in cherrypy.request.headers.items():
            if  key.lower().endswith('-dn') and val and val.find('CN=') != -1:
                user_dn = val
        if  user_dn:
            data = sitedbmgr.api_data('people_via_name')
            cols = data['desc']['columns']
            user = None
            for row in data['result']:
                rec = dict(zip(cols, row))
                if  rec['dn'] == user_dn:
                    user = rec['username']
                    break
            data = sitedbmgr.api_data('group_responsibilities')
            for uname, group, role in data['result']:
                if  uname == user and group == 'DAS':
                    thr = int(config.get(role, 5000))
    except:
        pass
    return thr

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
    if  system == 'dbs':
        bkg, col = '#008B8B', 'white'
    elif system == 'dbs3':
        bkg, col = '#006400', 'white'
    elif system == 'phedex':
        bkg, col = '#00BFBF', 'black'
    elif system == 'sitedb2':
        bkg, col = '#6495ED', 'black'
    elif system == 'runregistry':
        bkg, col = '#FF8C00', 'black'
    elif system == 'dashboard':
        bkg, col = '#DAA520', 'black'
    elif system == 'conddb':
        bkg, col = '#FFD700', 'black'
    elif system == 'reqmgr':
        bkg, col = '#696969', 'white'
    elif system == 'combined':
        bkg, col = '#FF69B4', 'black'
    elif system == 'tier0':
        bkg, col = '#AFEEEE', 'black'
    elif system == 'monitor':
        bkg, col = '#FF4500', 'black'
    else:
        keyhash = hashlib.md5()
        keyhash.update(system)
        bkg, col = '#%s' % keyhash.hexdigest()[:6], 'white'
    return bkg, col

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
        if  not conn or not is_db_alive(uri):
            try:
                conn = db_connection(uri)
                func()
                if  conn:
                    print "### db_monitor re-established connection %s" % conn
                else:
                    print "### db_monitor, lost connection"
            except:
                pass
        time.sleep(sleep)

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
            if  conn:
                print "### dascore_monitor, re-established connection \
                        %s, mapping.db records %s" % (conn, das)
            else:
                print "### dascore_monitor, lost connection"

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
    elif isinstance(data, HtmlString):
        res = unicode(data)
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

def checkarg(kwds, arg):
    """Check arg in a dict that it has str/unicode type"""
    data = kwds.get(arg, None)
    cond = data and (isinstance(data, str) or isinstance(data, unicode))
    return cond

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
                for key, val in jsondict.iteritems():
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
            if  checkarg(kwds, 'view'):
                if  kwds['view'] not in \
                        ['list', 'xml', 'json', 'plain', 'table']:
                    code  = web_code('Unsupported view')
                    raise HTTPError(500, 'DAS error, code=%s' % code)
            if  checkarg(kwds, 'collection'):
                if  kwds['collection'] not in ['cache', 'merge']:
                    code  = web_code('Unsupported collection')
                    raise HTTPError(500, 'DAS error, code=%s' % code)
            if  checkarg(kwds, 'msg') and not isinstance(kwds['msg'], str):
                code = web_code('Unsupported msg value')
                raise HTTPError(500, 'DAS error, code=%s' % code)
            if  checkarg(kwds, 'fid') and len(kwds['fid']) != 24:
                code = web_code('Unsupported id value')
                raise HTTPError(500, 'DAS error, code=%s' % code)
            if  checkarg(kwds, 'pid') and \
                    len(str(kwds['pid'])) not in [16, 24, 32]:
                code  = web_code('Unsupported pid value')
                raise HTTPError(500, 'DAS error, code=%s' % code)
            if  checkarg(kwds, 'ahash') and len(str(kwds['ahash'])) != 32:
                code  = web_code('Unsupported ahash value')
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
    def is_number(val):
        """Check if provided input is a number: int, long, float"""
        pat = number_pattern
        test = isinstance(val, int) or isinstance(val, float) or\
                isinstance(val, long) or \
                (isinstance(val, basestring) and \
                    pat.match(val.encode('ascii', 'ignore')))
        return test

    width = 100
    newline = '\n'
    orig_pad = pad
    sss = pad + '{' + newline
    for key, val in idict.iteritems():
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
        elif key == 'das':
            val['ts'] = presentation_datetime(val['ts'])
            val['expire'] = presentation_datetime(val['expire'])
            sss += ' "<b>das</b>": ' + json2html(val, pad=" "*3, ref=key)
        elif key == 'gridfs_id':
            value = "<a href=\"/das/gridfs?fid=%s\">%s</a>" \
                % (quote_plus(val), quote(val))
            sss += pad + value
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
            for idx in xrange(0, len(val)):
                item = val[idx]
                if  isinstance(item, dict):
                    sss += json2html(item, pad, ref=key)
                elif isinstance(item, list):
                    sss += str(item)
                else:
                    if  isinstance(item, NoneType):
                        sss += """%s<code class="null">None</code>""" \
                                % quote(ppp)
                    elif  is_number(item):
                        sss += """%s<code class="number">%s</code>""" \
                                % (quote(ppp), quote(item))
                    elif isinstance(item, basestring) \
                        and http_pattern.match(item):
                        sss += """%s<a href="%s">%s</a>""" \
                                % (quote(ppp), item, item)
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
            elif key == 'username':
                query = "user=%s" % val
                sss += """: <a href="/das/request?%s">%s</a>""" \
                        % (urllib.urlencode({'input':query}), quote(val))
            elif key == 'ip':
                query = "ip=%s" % val
                sss += """: <a href="/das/request?%s">%s</a>""" \
                        % (urllib.urlencode({'input':query}), val)
            elif is_number(val):
                sss += """: <code class="number">%s</code>""" % quote(val)
            elif isinstance(val, basestring) \
                and http_pattern.match(val):
                sss += """: <a href="%s">%s</a>""" % (val, val)
            else:
                sss += """: <code class="string">"%s"</code>""" % quote(val)
        if  key != idict.keys()[-1]:
            sss += ',' + newline
        else:
            sss += ""
    sss += newline + pad + '}'
    return sss

def das_json(record, pad=''):
    """
    Wrap provided jsonhtml code snippet into div/pre blocks. Provided jsonhtml
    snippet is sanitized by json2html function.
    """
    page  = """<div class="code"><pre>"""
    page += json2html(record, pad)
    page += "</pre></div>"
    return page

def gen_error_msg(kwargs):
    """
    Generate standard error message.
    """
    error  = "My request to DAS is failed\n\n"
    error += "Input parameters:\n"
    for key, val in kwargs.iteritems():
        error += '%s: %s\n' % (key, val)
    error += "Exception type: %s\nException value: %s\nTime: %s" \
                % (sys.exc_info()[0], sys.exc_info()[1], web_time())
    error = error.replace("<", "").replace(">", "")
    return error

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

