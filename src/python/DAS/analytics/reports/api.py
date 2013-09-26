#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=R0903
"""
This is intended to produce some statistics
about the API calls DAS is making, a la QueryReport.
The APICall records in analytics have limited
data however, so best attempt.
"""

from DAS.analytics.utils import Report, get_analytics_interface
from DAS.analytics.utils import nested_to_baobab
import collections

class APIReport(Report):
    "APIReport class"
    report_title = "APIs"
    report_info = "Information about DAS API calls"
    report_group = "General"

    def __call__(self, **kwargs):
        analytics = get_analytics_interface()

        counter = collections.defaultdict(lambda: collections.defaultdict(int))
        records = list(analytics.col.find({"api.name": {"$exists": True}}))

        for record in records:
            counter[record['system']][record['api']['name']] \
                += record['counter']

        api_plot = dict(central_label=False,
                        data=nested_to_baobab(counter),
                        external=False,
                        title="API Calls")

        return ("analytics_report_api", {"api_plot": api_plot})
