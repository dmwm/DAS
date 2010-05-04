
modules = ['services.logger']
#analyser(classname,analysername,kwarg0=0,kwarg1=1)
service('Logger')
service('Logger','CoreLogger',output='/tmp/das_analytics_core_log',subscriptions=[('all','core','log')])
