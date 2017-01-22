#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DAS option's class
"""

__author__ = "Gordon Ball"

class DASOption(object):
    """
    Class representing a single DAS option, independent of storage
    mechanism. Must define at least a section and name.

    In configparser

    [section]
    name = value

    In WMCORE config

    config.section.name = value

    The type parameter forces conversion as appropriate.
    The default parameter allows the option to not be specified
    in the config file. If default is not set then not providing
    this key will throw an exception.
    The validator parameter should be a single-argument function
    which returns true if the value supplied is appropriate.
    The destination argument is for values which shouldn't end
    up in config[section][name] = value but rather
    config[destination] = value
    Description is provided as a future option for some sort of
    automatic documentation.
    """
    def __init__(self, section, name, itype='string',
                 default=None, validator=None,
                 destination=None, description=''):
        self.section = section
        self.name = name
        self.type = itype
        self.default = default
        self.validator = validator
        self.destination = destination
        self.description = description
        self.value = None

        assert self.type in ('string', 'int', 'float', 'bool', 'list')

    def get_from_configparser(self, config):
        """
        Extract a value from a configparser object.
        """
        if config.has_section(self.section)\
            and config.has_option(self.section, self.name):
            value = None
            if self.type == 'string':
                if  sys.version.startswith('3.'):
                    value = config.get(self.section, self.name)
                else:
                    value = config.get(self.section, self.name, True)
            elif self.type == 'int':
                value = config.getint(self.section, self.name)
            elif self.type == 'float':
                value = config.getfloat(self.section, self.name)
            elif self.type == 'bool':
                value = config.getboolean(self.section, self.name)
            elif self.type == 'list':
                if  sys.version.startswith('3.'):
                    value = config.get(self.section, self.name).split(',')
                else:
                    value = config.get(self.section, self.name, True).split(',')

            if self.validator and not self.validator(value):
                msg = "Validation failed for option: %s.%s=%s" % \
                      (self.section, self.name, value)
                raise Exception(msg)

            self.value = value
            return value

        else:
            if not self.default == None:
                self.value = self.default
                return self.default
            else:
                msg = "Required option not found: %s.%s" % \
                      (self.section, self.name)
                raise Exception(msg)

    def write_to_configparser(self, config, use_default=False):
        """
        Write the current value to a configparser option. This
        assumes it has already been read or the values in self.value
        somehow changed, otherwise if use_default is set only
        those values with defaults are set.
        """
        value = self.value
        if use_default:
            if not self.default == None:
                value = self.default
            else:
                return

        if not config.has_section(self.section):
            config.add_section(self.section)
        if self.type == 'list':
            config.set(self.section, self.name, ','.join(value))
        elif self.type == 'boolean':
            config.set(self.section, self.name, 'true' if value else 'false')
        else:
            config.set(self.section, self.name, str(value))

    def get_from_wmcore(self, config):
        "As per get_from_configparser."
        if  self.section in config.listSections_() and \
            self.name in config.section_(self.section).dictionary_():
            value = config.section_(self.section).dictionary_()[self.name]
            if self.type == 'string':
                value = str(value)
            elif self.type == 'int':
                value = int(value)
            elif self.type == 'float':
                value = float(value)
            elif self.type == 'bool':
                value = bool(value)
            elif self.type == 'list':
                if not isinstance(value, (list, tuple)):
                    msg = 'Option %s.%s=%s: Not a list.' % \
                          (self.section, self.name, value)
                    raise Exception(msg)

            if self.validator and not self.validator(value):
                msg = "Validation failed for option: %s.%s=%s" % \
                      (self.section, self.name, value)
                raise Exception(msg)

            self.value = value
            return value

        else:
            if not self.default == None:
                self.value = self.default
                return self.default
            else:
                msg = "Required option not found: %s.%s" % \
                      (self.section, self.name)
                raise Exception(msg)

    def write_to_wmcore(self, config, use_default=False):
        "As per write_to_configparser."
        value = self.value
        if use_default:
            if not self.default == None:
                value = self.default
            else:
                return

        section = config.section_(self.section)
        setattr(section, self.name, value)
