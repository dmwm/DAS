#pylint: disable=R0903
"""
Test reports
"""

import time
from DAS.analytics.utils import Report

class TestReport(Report):
    """
    An example report, which does pretty much nothing.
    """
    report_title = "Test"
    report_group = "Test Reports"
    report_info = "Does very little."

    def __call__(self, **kwargs):
        period = float(kwargs.get('period', 3600))
        results = self.results.get_results(only="result",
                                           after=time.time()-period)
        return ("analytics_report_test", {"results": results})



