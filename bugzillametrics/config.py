"""Global config

"""

import io
import logging
from ConfigParser import ConfigParser, NoOptionError
from optparse import OptionParser



class Config(object):
    """Manage defaults and allow natural naming access to config, e.g.
    >>> value = config.group.key # natural naming
    instead of
    >>> value = config.get('group', 'key')
    
    """
    config_file = "/etc/bugzillametrics/bugzillametrics.cfg"
    def __init__(self, parser=ConfigParser()):
        """Set up defaults and user config.
        
        """
        # Read defaults
        defaults = open(self.config_file, 'r')
        parser.readfp(defaults)
        # Read user config if provided
        self.parser = parser

    def __getattr__(self, section_name):
        """Access options using a natural naming scheme.
        
        """
        parser = self.parser
        class Section(object):
            def __getattr__(self, option_name):
                return parser.get(section_name, option_name)
            def __setattr__(self, option_name):
                return parser.set(section_name, option_name)
        return Section()

    def options(self, section_name):
        return self.parser.options(section_name)

    def get_granularity(self):
        '''Returns granularity from config.
           Defaults to weeks if granularity is not present.'''
        days = {'days':1, 'weeks':7}
        try:
            granularity = self.parser.get('run','granularity')
        except NoOptionError:
            granularity = 'weeks'
        return (granularity, days[granularity])

    def update(self, args):
        "Handles command line options"
        optparser = OptionParser()

        optparser.add_option('-c', '--config', help='Pass an additional config file. Options specified in this file will override the defaults', metavar='file', type='string')
        optparser.add_option('-H', '--host', help='Database hostname', metavar='host', type='string')
        optparser.add_option('-u', '--user', help='Databse username', metavar='username', type='string')
        optparser.add_option('-p', '--passwd', help='Database password', metavar='passwd', type='string')
        optparser.add_option('-d', '--db', help='Database name', metavar='dbname', type='string')
        optparser.add_option('-o', '--outdir', help='Output dir (where the results will be stored)', metavar='outdir', type='string')
        optparser.add_option('-v', '--verbose', help='Increase log verbosity (debug mode)', action="store_true", default=False)
        optparser.add_option('--start', help='Phase to start from (dump/rebuild/count)', metavar='start_at', type='string')
        optparser.add_option('--stop', help='Phase to stop at (dump/rebuild/count)', metavar='stop_at', type='string')
        optparser.add_option('--today', help='Override current date', metavar='YYYY-MM-DD', type='string')
        optparser.add_option('--restrict', help='Restrict the dump to these bugs (comma separated list)', metavar='<bug1>,<bug2>,...', type='string')

        (options, _) = optparser.parse_args(args)
        # just a shorthand
        db = 'connection'
        phases = ['dump', 'rebuild', 'count']
        if options.config:
            # if additional config file is passed then use it
            self.parser.read(options.config)
        else:
            # Otherwise use the options passed if any
            if options.host:
                self.parser.set(db, 'host', options.host)
            if options.user:
                self.parser.set(db, 'user', options.user)
            if options.passwd:
                self.parser.set(db, 'password', options.passwd)
            if options.db:
                self.parser.set(db, 'database', options.db)
            if options.outdir:
                self.parser.set('workdir', 'path', options.outdir)
            if options.verbose:
                self.parser.set('workdir', 'log_level', 'debug')
            if options.start and options.start.lower() in phases:
                self.parser.set('run', 'start_at', options.start)
            if options.stop and options.stop.lower() in phases:
                self.parser.set('run', 'stop_at', options.stop)
            if options.today:
                self.parser.set('run', 'today', options.today)
            if options.restrict:
                self.parser.set('run', 'restrict', options.restrict)

            
