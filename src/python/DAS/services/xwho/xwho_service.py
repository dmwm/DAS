import re
import types
from DAS.services.abstract_service import DASAbstractService
from DAS.utils.utils import add2dict, map_validator
from DAS.utils.utils import row2das
from DAS.utils.regex import cms_tier_pattern
import DAS.utils.jsonwrapper as json
import urllib, urllib2
import time

class XWhoService(DASAbstractService):
    "Queries people by screen-scraping the CERN xwho enquiry"
    def __init__(self, config):
        DASAbstractService.__init__(self, 'xwho', config)
        self.map = self.dasmapping.servicemap(self.name)
        map_validator(self.map)
        
        self.re_summary_ids = re.compile(r'<a href="/xwho/people/([0-9]{6})">')
        
        self.re_find_name = re.compile(r'<h1>(.*?)</h1>')
        self.re_find_email = re.compile(r'<a href=mailto:(.*?)>')
        self.re_find_phone = re.compile(r'<b>Tel:</b>([0-9 ]+)')
    
    def api(self, query):
        """API implementation"""
        api = 'people' #this is the only API provided
        url = self.map[api]['url']
        expire = self.map[api]['expire']
        fakeargs = dict(self.map[api]['params'])
        realargs = {}
        
        name = query['spec'].get('person.name', None)
        if name: 
            fakeargs['name'] = name
            realargs['-notag'] = name
        phone = query['spec'].get('person.phone', None)
        if phone: 
            fakeargs['phone'] = phone
            realargs['-phone'] = phone
        login = query['spec'].get('person.login', None)
        if login:
            fakeargs['login'] = login
            realargs['-loginid'] = login
        
        self.logger.info("XWhoService::api(%s), name=%s, phone=%s, login=%s"\
                %(query, name, phone, login))
        
        start_time = time.time()
        try:
            req = urllib2.Request(url=url, 
                                  data=urllib.urlencode(realargs))
            raw = urllib2.urlopen(req, timeout=60).read()
            results = self.xwho_parser(raw)
        except:
            raise Exception("Error occurred during xwho fetching.")
        
        
        end_time = time.time()
        
        self.write_to_cache(query, expire, url, api, 
                            fakeargs, results, end_time-start_time)
        
        
    
    def xwho_parser(self, raw):
        """ service parser"""
        for match in self.re_summary_ids.findall(raw):
            ccid = int(match)
            person = {'ccid':ccid}
            person_raw = urllib2.urlopen(\
                self.map['people']['url']+'/'+str(ccid), timeout=60).read()
            
            name_match = self.re_find_name.search(person_raw)
            if name_match:
                name = name_match.group(1)
                if name.split(' ')[-2]=='from':
                    name = ' '.join(name.split(' ')[:-2])
                person['name'] = name.strip()
            phone_match = self.re_find_phone.search(person_raw)
            if phone_match:                
                person['phone'] = phone_match.group(1).strip()
            email_match = self.re_find_email.search(person_raw)
            if email_match:
                person['email'] = email_match.group(1).strip()
            #self.logger.info("XWhoService::xwho_parser %s"%person)
            yield {'person':person} 
             
            
        
