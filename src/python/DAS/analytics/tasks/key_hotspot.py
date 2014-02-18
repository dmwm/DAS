"""
KeyHotspot analyzer
"""

import collections
from DAS.analytics.tasks.hotspot_base import HotspotBase

class KeyHotspot(HotspotBase):
    """
    This analyzer runs infrequently (~daily) to determine
    based on a moving average of the whole period which
    keys (not their values) are most requested, and uses
    this to start several child ValueHotspot instances
    to monitor those keys, using the HotspotBase metrics.
    """
    def __init__(self, **kwargs):

        self.child_interval = kwargs.get('child_interval', 3600*4)
        self.child_preempt = kwargs.get('child_preempt', 300)
        HotspotBase.__init__(identifier="keyhotspot",
                             **kwargs)

    def generate_task(self, item, count, epoch_start, epoch_end):
        "Generate task"
        only_before = epoch_end + self.interval
        itemname = item.replace(r'.', '-')
        yield {'classname': 'ValueHotspot',
                'name': '%s-valuehotspot-%s' % (self.identifier, itemname),
                'only_before': only_before,
                'interval': self.child_interval,
                'kwargs':{'key': item,
                          'preempt':self.child_preempt}}

    def make_one_summary(self, start, finish):
        "Actually make the summary"

        items = collections.defaultdict(int)
        queries = self.das.analytics.list_queries(after=start,
                                                  before=finish)

        for query in queries:

            count = len(filter(lambda t: t >= start and t <= finish,
                               query['times']))
            for spec in query['mongoquery']['spec']:
                items[spec['key']] += count

        return items

