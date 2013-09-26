"""
Query report class
"""

from DAS.analytics.utils import Report, get_analytics_interface
from DAS.analytics.utils import nested_to_baobab
import time
import collections

class QueryReport(Report):
    """
    Display some information about queries submitted.
    """
    report_title = "DAS Query"
    report_info = "Summary information about recent DAS queries"
    report_group = "General"
    max_series_length = 100
    def __call__(self, **kwargs):
        period = int(kwargs.get('period', 7*86400))
        view_key = kwargs.get('key', None)
        now = time.time()
        analytics = get_analytics_interface()
        analyzer_exists = any([task['classname']=="QueryAnalyzer" \
                for task in self.scheduler.get_registry().values()])

        summaries = analytics.get_summary(\
                identifier="query_analyzer", after=now-period)
        # [ (query_structure, count) ]


        count_by_key = collections.defaultdict(int)
        field_count_by_key = collections.defaultdict(\
                        lambda: collections.defaultdict(int))
        instance_count = collections.defaultdict(int)
        constraint_by_key = collections.defaultdict(\
                        lambda: collections.defaultdict(int))

        seen_keys = set()

        time_bins = period / 3600
        if time_bins > self.max_series_length:
            time_bins = self.max_series_length
        time_interval = float(period) / time_bins
        time_series = collections.defaultdict(lambda: [0]*(time_bins+1))

        total_queries = 0
        for summary in summaries:
            midtime = 0.5*(summary['start']+summary['finish'])
            time_bin = int((midtime - (now - period)) / time_interval)
            for query in summary['queries']:
                if view_key and not view_key in query[0]['keys']:
                    continue
                count = query[1]
                total_queries += count
                for key in query[0]['keys']:
                    seen_keys.add(key)
                    constraint_by_key[key][query[0]['keys'][key]] += count
                    count_by_key[key] += count
                    for field in query[0]['fields']:
                        field_count_by_key[key][field] += count
                    time_series[key][time_bin] += count
                instance_count[query[0]['instance']] += count

        time_plot = dict(legend="topleft",
                         series=[dict(label=key, values=time_series[key]) \
                                for key in time_series],
                         title="Calls by time",
                         xaxis=dict(bins=time_bins+1,
                                    min=now-period,
                                    width=time_interval,
                                    label="Time",
                                    format="time"),
                         yaxis=dict(label="Queries"))


        constraint_plot = dict(central_label=False,
                               data=nested_to_baobab(constraint_by_key),
                               external=False,
                               title="Constraint by key")
        field_plot = dict(central_label=False,
                          data=nested_to_baobab(field_count_by_key),
                          external=False,
                          title="Field by key")
        instance_plot = dict(labels=True,
                             percentage=True,
                             series=[{'label':instance,
                                      'value': instance_count[instance]} \
                                        for instance in instance_count],
                             title="DBS Instance")
        key_plot = dict(labels=True,
                        percentage=True,
                        series=[{'label':key, 'value': count_by_key[key]} \
                                for key in count_by_key],
                        title="Key(s) used")

        popular_key = sorted(count_by_key, \
                key=lambda x: count_by_key[x])[-1] if count_by_key else None

        return ("analytics_report_query", {"nsummaries": len(summaries),
                                           "nqueries": total_queries,
                                           "view_key": view_key,
                                           "seen_keys": seen_keys,
                                           "analyzer_exists": analyzer_exists,
                                           "constraint_plot":constraint_plot,
                                           "field_plot":field_plot,
                                           "instance_plot":instance_plot,
                                           "key_plot":key_plot,
                                           "time_plot":time_plot,
                                           "period":period,
                                           "popular_key":popular_key})
