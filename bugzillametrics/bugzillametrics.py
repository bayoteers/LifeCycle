#!/usr/bin/env python
"""Service layer

"""

import logging
import os
from sys import argv

from config import Config
from rebuild import Rebuild
from count import Count
from cache import Cache
from dump import Dump
from ConfigParser import NoOptionError

class Runner(object):
    """
    
    """
    _stages = ('dump', 'rebuild', 'count')
    
    def __init__(self, config, args=None):
        """Load config and save dependencies.
        
        """
        self.config = config
        config.update(args)
        FORMAT='%(asctime)s\t%(levelname)s\t%(message)s'
        logging.basicConfig(filename=config.workdir.log_file,
            format=FORMAT,
            level=getattr(logging, config.workdir.log_level.upper()))
        logging.info('Config: ' + ', '.join(args))
        logging.info('Workdir: %s' % config.workdir.path)
        if os.path.exists(config.workdir.path):
            os.chdir(config.workdir.path)
        else:
            os.mkdir(config.workdir.path)    
            os.chdir(config.workdir.path)
        try:
            self.series_of_filters = self.get_filters(config)
        except NoOptionError:
            # Bail out in case there is no filters in config
            self.series_of_filters = None
        self.cache = Cache(config=config)

        
    def run(self):
        """Run stages.
        
        """
        config = self.config
        start_at = config.run.start_at
        stop_at = config.run.stop_at
        for skipped in self._stages[:self._stages.index(start_at)]:
            logging.info('Skipping stage: %s' % skipped)
        for stage in self._stages[self._stages.index(start_at):]:
            if stage == stop_at:
                logging.info('Stopping at stage: %s' % stage)
                break
            self.__getattribute__(stage)()
        
    def dump(self):
        """Dump Bugzilla data to PyTables.
        
        """
        dump = Dump(config=self.config, cache=self.cache)
        dump.run()
        
    def rebuild(self):
        """Rebuild complete bug history from Bugzilla data.
        
        """
        rebuild = Rebuild(config=self.config, cache=self.cache)
        rebuild.run()
    
    def get_filters(self, config):
        """Get filters from the config file and put them in a 
           convenient format
        """
        series_of_filters = []
        from_config = []

        for ft in config.options('filters'):
            from_config.append(config.filters.__getattr__(ft))

        for series in from_config:
            this_series = {}
            for kinds in series.split('&&'):
                ftype = kinds.split(':')[0].strip()
                vals = [ x.strip() for x in kinds.split(':')[1].split(',') ]
                this_series[ftype] = vals
            series_of_filters.append(this_series)
        logging.debug('Got this series of filters from config file: %s' % series_of_filters)
        return series_of_filters

    def count(self):
        """Collect results from rebuilt bug history.
        
        """
        count = Count(config=self.config, cache=self.cache)
        
        # All
        count.run()
        count.run(all=True)

        for series in self.series_of_filters:
            for fil_type in series.keys():
                for filter in series[fil_type]:
                    count.add_filter(fil_type, filter)
            count.run()
            count.run(all=True)
            count.flush_filters()

if __name__ == '__main__':
    # Do a full run
    runner = Runner(Config(), argv[1:])
    runner.run()
