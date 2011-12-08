#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
"""
DAS analytics configuration module
"""
import ConfigParser
import optparse
import re

from DAS.analytics.task import Task

class Option(object):
    """
    A single analytics option. This is meant to be defined
    elsewhere next to the classes that will then read it.

    The general format should be convertible to both optparse
    and file based config.

    Names should be valid python identifiers.
    """ 
    def __init__(self, name, group='General', 
                 short=None, default=None, type=None, 
                 choices=None, help=None, confonly=False):
        "Create a new Option"
        assert re.match(r'^[A-Za-z0-9_]+$', name)
        self.name = name
        self.short = short
        self.default = default
        self.type = type
        self.group = group
        self.choices = choices
        self.help = help
        self.confonly = confonly
    def __hash__(self): # to allow duplicates to be checked
        return hash((self.name,
                     self.short,
                     self.default,
                     self.type,
                     self.group,
                     self.help,
                     self.choices,
                     self.confonly))

    def dashed_name(self):
        "Substitute _ for - to make a more conventional command-line option"
        return self.name.replace('_','-')

    def validate(self, value):
        "Check the supplied value is in a list of choices and the right type"
        if isinstance(value, self.type):
            if self.choices:
                if value in self.choices:
                    return True
                else:
                    return 'Bad value for option "%s" (found "%s" valid %s).'\
                            % (self.name, value, self.choices)
            else:
                return True
        else:
            return 'Bad type for option "%s" (found "%s" expected "%s").'\
                    % (self.name, type(value), self.type)

class DASAnalyticsConfig(object):
    """
    This is both a container for all user-specified options,
    and the parser for options.

    Options are specified using the static method to add options,
    then evaluated first from the command line and then from
    any config files (supplied as positional arguments).

    Config files can be in ConfigParser/ini format (name.cfg)
    or literal python files (any other extension, or none), which
    are evaluated with execfile. This is the only way to add tasks.
    """
    type_map = {
        basestring: 'string',
        str: 'string',
        unicode: 'string',
        int: 'int',
        float: 'float'
    }
    options = {}

    @classmethod
    def add_option(cls, *args, **kwargs):
        "Add a new option. Accepts either an Option instance or arguments."
        if isinstance(args[0], Option):
            option = args[0]
        else:
            option = Option(*args, **kwargs)
            
        if not option.name in cls.options:
            cls.options[option.name] = option 

    def __init__(self):
        self._options = {}
        self._tasks = []

    def __getattr__(self, what):
        if what in self._options:
            return self._options[what]
        else:
            raise AttributeError 

    def get(self, name, default=None):
        "Get the value of a config option."
        return self._options.get(name, default)

    def add_task(self, name, classname, interval, 
                 only_once=False, max_runs=None, only_before=None,
                 **kwargs):
        "Add a task to be performed. Provided as Task() to execfile."
        #self.logger.info('Creating new task "%s"' % name)
        task = Task(name, classname, interval, kwargs,
                                   only_once, max_runs, only_before)
        if task in self._tasks:
            pass
        else:
            self._tasks.append(task)

    def get_tasks(self):
        return self._tasks

    def readfile(self, filename):
        "Read a config file and return the resulting dictionary."
        result = {}
        if filename.endswith('.cfg'):
            #self.logger.info("Reading %s (with ConfigParser)" % filename)
            cp = ConfigParser.SafeConfigParser()
            cp.read(filename)
            for section in cp.sections():
                for option in cp.options(section):
                    if option in self.options:
                        value = cp.get(section, option)
                        validation = self.options[option].validate(value)
                        if validation == True:
                            result[option] = value
                        else:
                            print validation
                            #self.logger.warning(validation)
        else:
            #self.logger.info("Reading %s (with execfile)" % filename)
            exec_globals = {'Task': self.add_task}
            execfile(filename, exec_globals)
            for option, value in exec_globals.items():
                if option in self.options:
                    validation = self.options[option].validate(value)
                    if validation == True:
                        result[option] = value
                    else:
                        print validation
                        #self.logger.warning(validation)
        return result

    def get_dict(self):
        """
        Return the options dictionary
        """
        return self._options

    def set_option(self, name, value):
        "Change an option after analytics start"
        if name in self.options:
            validation = self.options[name].validate(value)
            if validation == True:
                self._options[name] = value
                return True
            else:
                return validation
        else:
            return "Not a known option"

    def configure(self):
        """
        Accept command line options and parse any config files requested.
        Command line options take precedence over file options.
        """
        parser = optparse.OptionParser(
                    usage="%prog [options] conf_file0 conf_file1...")
        groups = list(set([opt.group 
                           for opt in self.options.values() 
                           if not opt.confonly]))
        names = []
        for grp in groups:
            group = optparse.OptionGroup(parser, grp)
            for option in self.options.values():
                if option.group == grp and not option.confonly:
                    args = ['--'+option.dashed_name()]
                    if option.short:
                        args.insert(0, '-'+option.short)
                    kwargs = {'dest':option.name}
                    if option.type and option.type in self.type_map:
                        kwargs['type'] = self.type_map[option.type]
                    elif option.type == bool:
                        kwargs['action'] = 'store_false'\
                                 if option.default else 'store_true'
                    if option.help:
                        kwargs['help'] = option.help
                    if option.choices:
                        kwargs['choices'] = option.choices
                        kwargs['type'] = 'choice'
                    group.add_option(*args, **kwargs)
                    names.append(option.name)
            parser.add_option_group(group)
        opts, args = parser.parse_args()

        defaults = {}
        for name, option in self.options.items():
            defaults[name] = option.default

        cmdresult = {} 
        for name in names:
            if hasattr(opts, name) and not getattr(opts, name) == None:
                cmdresult[name] = getattr(opts, name)

        fileresult = {}    
        for arg in args:
            fileresult.update(self.readfile(arg))

        defaults.update(fileresult)
        defaults.update(cmdresult)
        self._options.update(defaults)
