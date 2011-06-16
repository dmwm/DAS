#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
Collection of help cards for web UI.
"""

__author__ = "Valentin Kuznetsov"

import random
from DAS.web.utils import gen_color

def help_cards(base):
    """
    Return list of help cards used on web UI
    """
    hide = """
<div style="text-align: right">
<a href="javascript:HideTag('das_cards')">hide</a>
</div>
"""
    first = """
<h3 class="big">Help: DAS queries</h3>
DAS queries are formed by
<b>key=value</b> pairs, for example
<ul>
<li>dataset=*RelVal*</li>
<li>release=CMSSW_2_0_*</li>
<li>run=148126</li>
</ul>

The wild-card can be used to specify the pattern.
The list of supported DAS 
<b>keys</b> can be found in 
<b><a href="%s/services">Services</a></b> section.
For more details please read DAS 
<b><a href="%s/faq">Frequently Asked Questions</a></b>.
    """ % (base, base)
    first += hide

    card_list = []

    card = """
<h3 class="big">Help: DAS filters</h3>
Do you know that DAS supports filters? You can apply
<b>grep</b> filter to select part of the DAS record. For example,
if you look for a given dataset pattern, e.g. dataset=*RelVal*, 
you can select number of events as simple as
<pre>
dataset=*RelVal* | grep dataset.nevents
</pre>
Multiple filters can be applied together, for instance
<pre>
dataset=*RelVal* | grep dataset.name, dataset.nevents, dataset.nfiles
</pre>
    """
    card += hide
    card_list.append(card)

    card = """
<h3 class="big">Help: DAS filters and conditions</h3>
Do you know that DAS filters supports conditions? Their usage is trivial:
<pre>
file dataset=/a/b/c | grep file.name, file.size>3000000, file.size<6000000
</pre>
The DAS filters support: &lt;, &gt; and =. The usage of wild-card
is allowed for string patterns. For example, you can select record
attribute and apply a wild-card condition at the same time:
<pre>
dataset=*RelVal* | grep dataset.name, dataset.name=*RECO
</pre>
    """
    card += hide
    card_list.append(card)

    card = """
<h3 class="big">Help: DAS aggregators</h3>
DAS supports variety of aggregator functions, such as:
<b>min, max, sum, count, avg, median</b>. They can be applied in any
order to any DAS record attribute. For example:
<pre>
file dataset=/a/b/c |  max(file.size), min(file.size),avg(file.size),median(file.size)
</pre>
Custom map-reduce function are also supported. Please contact DAS 
<b><a href="https://svnweb.cern.ch/trac/CMSDMWM/newticket?component=DAS&summary=Request map reduce function&owner=valya">support</a></b> if you need one.
    """
    card += hide
    card_list.append(card)

    card = """
<h3 class="big">Help: DAS date usage</h3>
DAS has special <b>date</b> keyword. It accepts values either in
YYYYMMDD format or via <b>last</b> operator. For example:
<pre>
run date=20110316
run date last 24h
jobsummary date last 60m
</pre>
Supported units for <b>last</b> operator are <b>d</b> (days),
<b>h</b> (hours) and <b>m</b>(minutes).
    """
    card += hide
    card_list.append(card)

    card = """
<h3 class="big">Help: DAS special keywords</h3>
DAS has special <b>records</b> and <b>queries</b> keywords.
The former can be used to look-up all records in DAS cache, while
later shows most recent queries placed in DAS. The DAS filters
can be used with them too. For example, if you want to see if there
are any file records in DAS cache you can simply type:
<pre>
records | grep file.name
</pre>
    """
    card += hide
    card_list.append(card)

    card = """
<h3 class="big">Help: DAS command line interface</h3>
Do you know that you can use DAS from your terminal? Go to
<b><a href="%s/cli">CLI</a></b> link and save it on your disk, e.g. as <em>das_cli</em>.
Then you can use it as simple as
<pre>
python das_cli --query="dataset=/ExpressPhysics*"
</pre>
Please use <b>--help</b> for more options.
The default output format is JSON. But you can apply filter and use plain data format, e.g.:
<pre>
python das_cli --query="dataset=/ExpressPhysics* | grep dataset.name, dataset.nevents" --format=plain
/ExpressPhysics/Run2011A-Express-v2/FEVT 13321290
...
</pre>
    """ % base
    card += hide
    card_list.append(card)

    card = """
<h3 class="big">Help: DAS colors</h3>
Colored little box, e.g.
<cite style="background-color:%s;padding-left:7px; padding-right:7px">&nbsp;</cite>
next to the Record output 
represents specific CMS data-services.<br />
Please follow DAS color map: <u style="color:#8f1f08;">show</u>
<u style="color:#8f1f08;">hide</u>
links to find out which system it corresponds to.

<br/>
<br/>

If you see several colored boxes in a row, e.g.
<cite style="background-color:%s;padding-left:7px; padding-right:7px">&nbsp;</cite>
<cite style="background-color:%s;padding-left:7px; padding-right:7px">&nbsp;</cite>,
it means that this
record is aggregated across several CMS data-services.
    """ % (gen_color('phedex'), gen_color('dbs'), gen_color('phedex'))
    card += hide
    card_list.append(card)

    card = """
<h3 class="big">Help: Commonly used dataset queries</h3>
Most physicists are interested to find their desired dataset. Here is an incomplete
list of queries which cover this use-case:
<pre>
dataset=*Zee*
dataset release=CMSSW_4*
dataset release=CMSSW_4* datatype=mc
dataset dataset=*Zee* datatype=mc release=CMSSW_4*
dataset primary_dataset=ZJetToEE_Pt* tier=*GEN*
dataset group=Top datatype=mc
dataset run=148126
dataset dataset=/Cosmics/Run2010B* site=T1_US_FNAL
</pre>
"""
    card += hide
    card_list.append(card)

    card = """
<h3 class="big">Help: Free text based queries</h3>
If you type free text based query, e.g.
<pre>
Zee CMSSW_3
</pre>
DAS will match provided keywords with
appropriate DAS <b>key=value</b> pairs which will displayed in the input field,
in this case 
<pre>dataset=*Zee* release=CMSSW_3*</pre>
<b>Please note:</b> DAS queries are case-sensitive due to underlying 
restrictions from participated data-services, but in
<b>autocompletion</b> mode DAS will apply case-insensitive searches for
all valid datasets to find your match.
"""
    card += hide
    card_list.append(card)

    random.shuffle(card_list)
    cards = [first] + card_list
    return cards
