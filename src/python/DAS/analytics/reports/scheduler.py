"""
Scheduler reports
"""

import time
from DAS.analytics.utils import Report

class SchedulerReport(Report):
    "Shows some load-average like stats for the scheduler"
    report_title = "Scheduler"
    report_info = "Load averages and success rates of the scheduler"
    summary_intervals = (600, 3600, 6*3600, 24*3600)
        
    def __call__(self, **kwargs):
        """
        No caching is done here, but these are potentially quite expensive.
        """        
        result = {}
        for interval in self.summary_intervals:
            result[interval] = {}

            jobs = self.results.get_results(\
                only="result", after=time.time()-interval)
            result[interval]['jobs'] = len(jobs)
            result[interval]['success'] = \
                sum([1 for j in jobs if j['success']])
            result[interval]['failed'] = \
                result[interval]['jobs'] - result[interval]['success']
            result[interval]['time'] = \
                sum([j['finish_time']-j['start_time'] for j in jobs])
            result[interval]['load'] = result[interval]['time']/interval
            result[interval]['unique'] = \
                len(set([j['master_id'] for j in jobs]))

        return ("analytics_report_scheduler", {"summary": result})
