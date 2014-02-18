"""
Hotspot report class
"""

from DAS.analytics.utils import Report, get_analytics_interface
import time
import collections

gen_identifier = \
lambda task: "valuehotspot-%s" \
% (task['kwargs'].get('key', '').replace('.', '-'))

class HotspotReport(Report):
    """
    A report showing the data collected by Hotspot instances and
    the resulting selection of keys being pre-fetched.
    """
    report_title = "Hotspot"
    report_info = "Show some plots and information relating "
    report_info += "to the hotspot-type analyzers, if any"
    report_group = "Tasks"
    max_series_length = 100
    def __call__(self, **kwargs):
        identifier = kwargs.get('identifier', None)
        if identifier:
            return self.hotspot_report(**kwargs)
        else:
            return self.list_hotspots()

    def hotspot_report(self, **kwargs):
        "Hotspot report"
        analytics = get_analytics_interface()
        identifier = kwargs['identifier']


        taskdicts = [task for task in \
                self.scheduler.get_registry().values() \
                        if 'Hotspot' in task['classname'] and \
                        gen_identifier(task) == identifier]
        taskobj = None
        if taskdicts:
            taskobj = taskdicts[0]


        period = 86400*30
        interval = 3600*4
        fraction = 0.15
        if taskobj:
            period = taskobj['kwargs'].get('period', 86400*30)
            interval = taskobj['interval']
            fraction = taskobj['kwargs'].get('fraction', 0.15)

        period = int(kwargs.get('period', period))
        fraction = float(kwargs.get('fraction', fraction))

        epoch_end = time.time()
        epoch_start = time.time() - period

        summaries = analytics.get_summary(identifier,
                                          after=epoch_start,
                                          before=epoch_end)

        counter = collections.defaultdict(int)
        map(lambda x: counter.update(x['keys']), summaries)

        sorted_keys = sorted(counter, key=lambda x: counter[x])
        total_calls = float(sum(counter.values()))

        key_series = []
        call_count = 0
        for key in sorted_keys:
            call_count += counter[key]
            key_series += [call_count]

        binning = 1
        if len(key_series) > self.max_series_length:
            binning = len(key_series)/self.max_series_length
            key_series = key_series[len(key_series)%binning-1::binning]

        key_plot = dict(legend="null",
                        series=[dict(colour="#ff0000",
                                     label="Calls",
                                     values=key_series)],
                        title="Cumulative calls by key",
                        xaxis=dict(bins=len(key_series)-1,
                                   label="Keys",
                                   width=binning,
                                   min=0),
                        yaxis=dict(label="Cumulative calls"))

        summary_durations = [s['finish'] - s['start'] for s in summaries]
        summary_density = [len(s['keys']) for s in summaries]

        summary_plot = dict(legend="null",
                            series=[dict(colour="#ff0000",
                                         label="Summaries",
                                         marker="*",
                                         x=summary_durations,
                                         y=summary_density)],
                            title="Summary length and call count",
                            xaxis=dict(label="Summary length"),
                            yaxis=dict(label="Number of calls"))


        time_bins = int((epoch_end - epoch_start) / interval)
        time_interval = interval
        if time_bins > self.max_series_length:
            time_bins = self.max_series_length
            time_interval = (epoch_end - epoch_start) / time_bins
        time_series = [0]*(time_bins+1)

        for sss in summaries:
            bin = int(((0.5*(sss['finish']+s['start'])) - epoch_start)\
                      / time_interval)
            time_series[bin] += len(s['keys'])

        time_plot = dict(legend="null",
                         series=[dict(colour="#ff0000",
                                      label="Query density over time",
                                      values=time_series)],
                         title="Query density over time",
                         xaxis=dict(bins=time_bins+1,
                                    label="Time",
                                    format="time",
                                    min=epoch_start,
                                    width=time_interval),
                         yaxis=dict(label="Calls"))

        selected = []
        cumulative = 0
        while sorted_keys and cumulative < fraction * total_calls:
            key = sorted_keys.pop()
            cumulative += counter[key]
            selected += [key]


        return ("analytics_report_hotspot", {"list":False,
                                             "identifier":identifier,
                                             "task":taskobj,
                                             "nkeys":len(counter.keys()),
                                             "ncalls":total_calls,
                                             "nsummaries":len(summaries),
                                             "key_plot":key_plot,
                                             "summary_plot":summary_plot,
                                             "time_plot":time_plot,
                                             "selected":selected})

    def list_hotspots(self):
        "List hotstpots"
        identifiers = set([task['kwargs'].get('identifier') \
                for task in self.scheduler.get_registry().values() \
                        if 'Hotspot' in task['classname']])
        return ("analytics_report_hotspot", {"list":True,
                                             "identifiers":list(identifiers)})





