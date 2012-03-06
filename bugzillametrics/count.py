"""

"""

import logging
from datetime import datetime, timedelta, date
from collections import defaultdict
from pprint import pprint as pp
from copy import deepcopy
from sys import stdout


class Count(object):
    """
    
    """
    #_debug = (255766, 249267, 255125,)
    _debug = tuple()
    endpoints = dict(overall=('new', 'closed'),
        fixing=('new', 'resolved+fixed'),
        releasing=('resolved+fixed', 'released'),
        verification_fixed=('released+fixed', 'verified+fixed'),
        verification_unfixed=('resolved-fixed'  , 'verified'),
        waiting=('waiting', 'resolved'))
    # XXX: generate from endpoints
    find_youngest = frozenset(['new'])
    find_oldest = frozenset(['closed', 'released', 'resolved', 'waiting', 'verified'])
    find_oldest_fixed = frozenset(['resolved', 'verified', 'released'])
    find_oldest_unfixed = frozenset(['resolved'])
    all = False
    filter = {}
    filter_type = []
    allow_severity = set(['blocker', 'critical', 'major', 'normal', 'minor'])
    
    def __init__(self, config, cache):
        """
        
        """
        self.config = config
        self.cache = cache
        self.history = cache.file.root.history
        self.interesting = frozenset(['new']) \
            .union(self.find_oldest) \
            .union(self.find_oldest_fixed)
        self.granularity, self.days = self.config.get_granularity()
        
    @staticmethod
    def group_by(field, history):
        """
        
        """
        group = []
        names = history.colnames
        last_group_id = None
        i = 1
        end = len(history)
        for coord in history:
            group_id = coord[field]
            if (group_id == last_group_id or last_group_id is None) and i < end:
                last_group_id = group_id
                group.append(dict(zip(names, coord[:])))
                i += 1
                continue
            yield last_group_id, group
            last_group_id = group_id
            # TODO: DRY (?)
            group = [dict(zip(names, coord[:]))]

    def _to_tuple(self, date):
        '''Convert the given date to the tuple according 
           to what period type we want'''
        if self.granularity == 'days':
            return tuple(date.isoformat()[:10].split('-'))
        elif self.granularity == 'weeks':
            return date.isocalendar()[:2]
        
    
    def run(self, all=False):
        """

        """
        self.all = all
        stage = []
        for ft in self.filter.keys():
            stage.append('%s/%s' % (ft, (',').join(self.filter[ft])))

        stage = (' | ').join(stage)

        if not stage:
            stage = 'No filters'

        logging.info('Stage: count (%s)' % stage)
        
        # Set up period container template
        if self.config.run.today:
            run_until = datetime.strptime(self.config.run.today, '%Y-%m-%d').date()
        else:
            run_until = date.today()
        # TODO: automate
        self.today = run_until
        period_range = range(int(self.config.run.period_range))
        periods = {}
        _date = run_until
        endpoints = self.endpoints
        for i in period_range:
            _date -= timedelta(days=self.days)
            periods[self._to_tuple(_date)] = defaultdict(set)
        component_results = {}
        # Initializing component if we don't find anything
        component = ''
        
        _debug = self._debug
        for id, group in self.group_by('id', self.history):
            if _debug and id not in _debug:
                continue
            r = self.collect(id, group)
            
            # XXX
            if not r:
                continue
            if not self.all:
                component = r['component']
            else:
                component = 'All'
            c = component_results.setdefault(component, deepcopy(periods))
            
            for cycle, (start, end) in endpoints.iteritems():
#                _start = start
                start, end = r.get(start, None), r.get(end, None)
#                if _start == 'resolved-fixed':# and end <= start:
#                    print start, end
                if not start or not end or start > end:
                    continue
                end_periodtuple = self._to_tuple(end)
                if end_periodtuple in c:
                    delta = (end - start).days
                    c[end_periodtuple][cycle].add((delta, id))
        if component == 'All':
            x = 'all'
        elif component != '':
            x = 'components'
        else:
            x = ''
        if x:
            if self.filter:
                for fil_type in self.filter.keys():
                    for fil in self.filter[fil_type]:
                        x += '-' + fil
            html = open('lifecycles-%s.html' % (x,), 'w')
            self._export_html(component_results, html)
            csv = open('lifecycles-%s.csv' % (x,), 'w')
            self._export(component_results, csv)
        else:
            logging.warning('Not generating output for %s as we didn\'t '
                            'find any bug with this criteria' % self.filter)
        self.all = False
            
    def _export(self, results, out=stdout):
        """
        
        """
        d = ';'
        keys = self.endpoints.keys()
        keys.sort()
        if self.granularity == 'days':
            print >> out, 'Component', d, 'Year', d, 'Month', d, 'Day', d,
        else:
            print >> out, 'Component', d, 'Year', d, 'Week', d,
        print >> out, d.join(map(str.title, keys)), d
        for component_name, component in results.iteritems():
            for periodtuple in sorted(component.keys()):
                periodresults = component[periodtuple]
                if self.granularity == 'days':
                    year, month, day = periodtuple
                    print >> out, component_name, d, year, d, month, d, day, d,
                else:
                    year, week = periodtuple
                    print >> out, component_name, d, year, d, week, d,
                for k in keys:
                    dt = periodresults[k]
                    dt = [x[0] for x in dt]
                    dt_n = len(dt)
                    dt_sum = sum(dt)
                    if dt_n:
#                        dr = '%.2f (%d / %d)' % (dt_sum / float(dt_n), dt_sum, dt_n)
                        dr = '%.2f' % (dt_sum / float(dt_n),)
                    else:
                        dr = ''
                    print >> out, dr, d,
                print >> out, ''
                
    def _export_html(self, results, out=stdout):
        """
        
        """
        print >> out, '<table><thead><tr>'
        d = '</td><td>'
        dh = '</th><th>'
        dhs = '</th><th>'
        keys = self.endpoints.keys()
        keys.sort()
        if self.granularity == 'days':
            print >> out, '<th>Component', dh, 'Year', dh, 'Month', dh, 'Day',
        else:
            print >> out, '<th>Component', dh, 'Year', dh, 'Week',
        for key in map(str.title, keys):
            print >> out, '<th title="%s &ndash; %s">' % self.endpoints[key.lower()],
            print >> out, key, '</th>',
#        print dhs.join(map(str.title, keys)), '</th></tr></thead><tbody>'
        for component_name, component in results.iteritems():
            for periodtuple in sorted(component.keys()):
                periodresults = component[periodtuple]
                print >> out, '<tr><td>',
                if self.granularity == 'days':
                    year, month, day = periodtuple
                    print >> out, component_name, d, year, d, month, d, day, d,
                else:
                    year, week = periodtuple
                    print >> out, component_name, d, year, d, week, d,
                z = []
                for k in keys:
                    pdr = periodresults[k]
                    dt = [x[0] for x in pdr]
                    dt_n = len(dt)
                    bugy = map(str, [x[1] for x in pdr])
                    if dt_n:
                        median, medianb = sorted(pdr)[dt_n / 2]
                        url = '%2C'.join(bugy)
                        bugli = '(' + (' + '.join(map(str, dt))) + ') / ' + str(dt_n)
                        url = 'http://bugzilla.org/buglist.cgi?quicksearch=' + url
                        urlm = 'https://bugzilla.org/bugzilla/show_activity.cgi?id=' + str(medianb)
                        dt_sum = sum(dt)
                        dr = """<span title="%(bugli)s"><a href="%(url)s">%(avg).2f</a> 
                           (%(dt_sum)d / %(dt_n)d)</span>""" \
                            % dict(url=url, urlm=urlm, bugli=bugli,
                                dt_n=dt_n, dt_sum=dt_sum, avg=dt_sum / float(dt_n),
                                median=median)
                    else:
                        dr = ''
                    z.append(dr)
                print >> out, d.join(z)
                print >> out, '</td></tr>'
        print >> out, '</tbody></table>'

    def add_filter(self, fil_type, filter):
        '''Add a filter of the given type'''
        #FIXME: use setdefault
        if fil_type in self.filter_type:
            # filter_type already exists
            self.filter[fil_type].append(filter)
        else:
            # filter_type was not there yet
            self.filter_type.append(fil_type)
            self.filter[fil_type] = [filter]

    def flush_filters(self):
        'Flush any filter that was previously defined'
        self.filter = {}
        self.filter_type = []

    def collect(self, bug_id, bug_history):
        """
        
        """
        oldest_coord = {}
        today = self.today
        find_oldest = self.find_oldest
        find_oldest_fixed = self.find_oldest_fixed
        find_oldest_unfixed = self.find_oldest_unfixed
        interesting = self.interesting
        r = None
        
        filter = self.filter
        filter_type = self.filter_type
        
            
        # Filter severity
        severity = bug_history[0]['severity']
        # TODO: do at rebuilding stage
        if not severity in self.allow_severity:
            return {}
        if 'severity' in filter_type and not severity in filter['severity']:
            return {}
        
        for coord in bug_history:
            status = coord['status']
            # Filter keywords
            if 'keywords' in filter_type :
                # XXX: preprocess
                # FIXME: Proabably stripping and lowering not needed anymore
                keywords = frozenset(map(str.lower, map(str.strip, 
                    coord['keywords'].split(','))))
                if not keywords.intersection(set(filter['keywords'])):
                    continue

            # Filter flags
            if 'flags' in filter_type:
                flagset = set([x+'+' for x in filter['flags']])
                curflags = set(coord['flags'].split(','))
                if not flagset.intersection(curflags):
                    continue
            fixed_status = status + '+fixed'
            unfixed_status = status + '-fixed'
            if not status in interesting:
                # Skip boring coordinates
                continue
            # Convert UNIX time to date because times are cached as integers
            # Not automated for performance reasons
            ts = date.fromtimestamp(coord['ts'])
            # TODO: prefilter
            # TODO: prefilter at grouping stage
            if ts >= today:
                continue
            # Get or instantiate result subcontainer
            # TODO: use defaultdict
            r = oldest_coord.setdefault(bug_id, dict(component=coord['component'],
                new=date.fromtimestamp(coord['creation_ts'])))
            
            if not 'status' in coord['changed_fields'].split('||'):
                # This was not a real status change
                continue

            # Collect oldest coordinates
            if not status in r \
                and status in find_oldest: 
                r[status] = ts
            if not fixed_status in r \
                and status in find_oldest_fixed \
                and coord['resolution'] == 'fixed':
                r[fixed_status] = ts
            if not unfixed_status in r \
                and status in find_oldest_unfixed \
                and not coord['resolution'] == 'fixed':
                r[unfixed_status] = ts
#            if not unfixed_status in r \
#                and status in find_oldest_unfixed:
#                    if not coord['resolution'] == 'fixed':
#                    print 'xxx', status
            
#            if coord['resolution'] and not coord['resolution'] == 'fixed':
#                pfp = (status, coord['resolution'])
#                if status == 'resolved' and not coord['resolution'] == 'fixed':
#                    print status, coord['resolution'], ts, unfixed_status in r, unfixed_status, r[unfixed_status]
#                xq.setdefault(pfp, 0)
#                xq[pfp] += 1
#            if status in find_youngest:
#                r[status] = ts
#        pp(bug_id)
#        pp(r)
        return r
