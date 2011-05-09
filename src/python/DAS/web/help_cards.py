#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
Collection of help cards for web UI.
"""

__author__ = "Valentin Kuznetsov"

import random

def help_cards(base):
    """
    Return list of help cards used on web UI
    """
    first = """
<h3 class="big">DAS queries</h3>
If you new to DAS try to search desired data by using 
<b>key=value</b> pairs, for example
<ul>
<li>dataset=*RelVal*</li>
<li>release=CMSSW_2_0_*</li>
<li>run=148126</li>
</ul>

The list of supported DAS 
<b>keys</b> can be found in 
<b><a href="%s/services">Services</a></b> section.
For more details please read DAS 
<b><a href="%s/faq">Frequently Asked Questions</a></b>.
    """ % (base, base)

    card_list = []

    card = """
<h3 class="big">DAS filters</h3>
Do you know that DAS supports filters? Similar to UNIX you can apply
<b>grep</b> filter to select certain data. For example,
if you look for a given dataset=*RelVal*, you can select its number of events
as simple as
<pre>
dataset=*RelVal* | grep dataset.nevents
</pre>
Multiple filters can be applied together, for instance
<pre>
dataset=*RelVal* | grep dataset.name, dataset.nevents, dataset.nfiles
</pre>
    """
    card_list.append(card)

    card = """
<h3 class="big">DAS filters and conditions</h3>
Do you know that DAS filters supports conditions. Their usage is trivial:
<pre>
file dataset=/a/b/c | grep file.name, file.size>3000000, file.size<6000000
</pre>
The DAS filters supports: &lt;, &gt; and =. The usage of wild-card
is allowed over string patterns. For example, you can select record
attribute and apply wild-card condition at the same time:
<pre>
dataset=*RelVal* | grep dataset.name, dataset.name=*RECO
</pre>
    """
    card_list.append(card)

    card = """
<h3 class="big">DAS aggregators</h3>
DAS supports variety of aggregator functions, such as:
<b>min, max, sum, count, avg, median</b>. They can be applied in any
order to any DAS record attribute. For example:
<pre>
file dataset=/a/b/c |  max(file.size), min(file.size),avg(file.size),median(file.size)
</pre>
Custom map-reduce function are also supported. Please contact DAS 
<b><a href="https://svnweb.cern.ch/trac/CMSDMWM/newticket?component=DAS&summary=Request map reduce function&owner=valya">support</a></b> if you need one.
    """
    card_list.append(card)

    card = """
<h3 class="big">DAS date usage</h3>
DAS has a special <b>date</b> keyword. It can accepts values either in
YYYYMMDD format or via <b>last</b> operator. For example:
<pre>
run date=20110316
run date last 24h
jobsummary date last 60m
</pre>
Supported units for <b>last</b> operator are <b>d</b> (days),
<b>h</b> (hours) and <b>m</b>(minutes).
    """
    card_list.append(card)

    card = """
<h3 class="big">DAS special keywords</h3>
DAS has special <b>records</b> and <b>queries</b> keywords.
The former can be used to look-up all records in DAS cache, while
later shows most recent queries placed into DAS. The DAS filters
can be used with them too. For example, if you want to see if there
are any file records in DAS cache you can simply type:
<pre>
records | grep file.name
</pre>
    """
    card_list.append(card)

    card = """
<h3 class="big">DAS command line interface</h3>
Do you know that you can use DAS from your terminal. Go to
<b><a href="%s/cli">CLI</a></b> link and save it on your disk, e.g. as <em>das_cli</em>.
Then you can use it as simple as
<pre>
python das_cli --query="dataset=/ExpressPhysics*"
</pre>
The default format is JSON. But you can apply filter and use plain data format, try
the following:
<pre>
python das_cli --query="dataset=/ExpressPhysics* | grep dataset.name, dataset.nevents" --format=plain
/ExpressPhysics/Run2011A-Express-v2/FEVT 13321290
/ExpressPhysics/Run2011A-Express-v1/FEVT 9654748
...
</pre>
    """ % base
    card_list.append(card)

    random.shuffle(card_list)
    cards = [first] + card_list
    return cards
