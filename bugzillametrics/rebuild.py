"""

"""
import logging
from collections import defaultdict
from pprint import pprint as pp
from datetime import date,datetime

from cache import Coord
from dump import Dump

class Rebuild(object):
    """
    
    """
    def __init__(self, config, cache):
        """
        
        """
        self.config = config
        self.cache = cache
        # TODO: don't access cache.file directly
        self.root = self.cache.file.root
        self.additive_fields = ['flags', 'keywords']
        self.closed_states = ['resolved', 'verified', 'closed', 'pre_verified', 'released']
        
    def _remove_history(self):
        """
        
        """
        root = self.root
        if 'history' in root:
            root.history.remove()
        
    def _reset_history(self):
        """
        
        """
        self._remove_history()
        self.root.history = self.cache.file.createTable('/', 'history', Coord, 'Bug coordinates')

    def set_next_events(self, ts, group, field, added, will_change):
        '''set next events in case they were not like they should have been'''
        gk = group.keys()
        gk.sort(cmp=lambda x,y:y-x)
        rem = 'removed'
        ch_flds = 'changed_fields'
        for next_ts in gk:
            if next_ts >= ts:
                event = group[next_ts]
                if field in self.additive_fields:
                    if added != '':
                        flags_to_add = added.split(',')
                        for fgta in flags_to_add:
                            if will_change[field].has_key(fgta):
                                #print "(found key %s for will_change): at %s, %s will change on %s" % (field, datetime.fromtimestamp(next_ts), field, datetime.fromtimestamp(will_change[field][fgta]))
                                if next_ts == will_change[field][fgta]:
                                    #print "next_ts: %s, will_change/%s/%s: %s" % (datetime.fromtimestamp(next_ts), field, fgta, datetime.fromtimestamp(will_change[field][fgta]))
                                    #print '===trying to set %s as %s for ts:%s' % (rem, fgta.lower(), datetime.fromtimestamp(next_ts))
                                    if event.has_key(rem):
                                        if event[rem] != '':
                                            removed = dict([x.split('>>') for x in event[rem].split('||')])
                                            self.update_this(field, ['', fgta.lower()], removed, will_change, next_ts)
                                            event[rem] = '||'.join(['%s>>%s' % (x, removed[x]) for x in removed])
                                    else:
                                        event[rem] = '%s>>%s' % (field,added.lower())

                                    if event.has_key(ch_flds):
                                        changed_fields = event[ch_flds].split('||')
                                        changed_fields.append(field)
                                        event[ch_flds] = '||'.join(changed_fields)
                                    else:
                                        event[ch_flds] = field
                                        
                                if next_ts < will_change[field][fgta]:
                                    self.update_this(field, ['', fgta.lower()], event, will_change, next_ts)
                            else:
                                #print '(no key %s for will_change): trying to set %s as %s for ts:%s' % (field, field, fgta.lower(), datetime.fromtimestamp(next_ts))
                                self.update_this(field, ['', fgta.lower()], event, will_change, next_ts)
                                
                else:
                    if will_change.has_key(field):
                        #print "(found key %s for will_change): at %s, %s will change on %s" % (field, datetime.fromtimestamp(next_ts), field, datetime.fromtimestamp(will_change[field]))
                        if next_ts < will_change[field]:
                            self.update_this(field, ['', added.lower()], event, will_change, next_ts)
                        if next_ts == will_change[field]:
                            #print '===trying to set %s as %s for ts:%s' % (rem, added.lower(), datetime.fromtimestamp(next_ts))
                            if event.has_key(rem):
                                if event[rem] != '':
                                    removed = dict([x.split('>>') for x in event[rem].split('||')])
                                    removed[field] = added.lower()
                                    event[rem] = '||'.join(['%s>>%s' % (x, removed[x]) for x in removed])
                            else:
                                event[rem] = '%s>>%s' % (field,added.lower())
                            if event.has_key(ch_flds):
                                changed_fields = event[ch_flds].split('||')
                                changed_fields.append(field)
                                event[ch_flds] = '||'.join(changed_fields)
                            else:
                                event[ch_flds] = field
                    else:
                        #print '(no key %s for will_change): trying to set %s as %s for ts:%s' % (field, field, added.lower(), datetime.fromtimestamp(next_ts))
                        event[field] = added.lower()
                        will_change[field] = ts

    def update_this(self, field, changes, event, will_change, ts):
        """updates this to event[field]
           Takes care of additive fields, like keywords and flags, properly
           
           changes is a list of [to_be_removed, to_be_added] fields"""
        if field in self.additive_fields:
            if changes[1] != '':
                add_fields = changes[1].split(',')
                #print "adding %s to %s" % (','.join(add_fields), field)
                if event.has_key(field):
                    if event[field] != '':
                        cur_flags = event[field].split(',')
                        event[field] = ','.join(list(set.union(set(add_fields),set(cur_flags))))
                    else:
                        event[field] = ','.join(add_fields)
                else:
                    event[field] = ','.join(add_fields)
                #print field,':',event[field]
                for wc in add_fields:
                    #if will_change[field].has_key(wc):
                        #print "[update_add] updating will_change for %s/%s to %s (was %s)" % (field,wc,datetime.fromtimestamp(ts), datetime.fromtimestamp(will_change[field][wc]))
                    #else:   
                        #print "[update_add] updating will_change for %s/%s to %s (wasn't there) " % (field,wc,datetime.fromtimestamp(ts))
                    will_change[field][wc] = ts
            if changes[0] != '':
                rem_fields = changes[0].split(',')
                #print "removing %s from %s" % (','.join(rem_fields), field)
                if event.has_key(field):
                    if event[field] != '':
                        cur_flags = event[field].split(',')
                        #print 'current flags: ', cur_flags,' - rem_fields: ', rem_fields
                        event[field] = ','.join(list(set.difference(set(cur_flags),set(rem_fields))))
                        #print ' is ', event[field]

                    #else:
                        #print "event empty, can't remove anything from there"
                        #print "Need to set it also for all the other next events!!"
                #else:
                    ##print "event empty, can't remove anything from there"
                    #print "Need to set it also for all the other next events!!"
        elif not field in self.additive_fields:
            #print "Setting %s for %s" % (changes[1], field)
            event[field] = changes[1]
            will_change[field] = ts

    def run(self):
        """
        
        """
        logging.info('Stage: rebuild history')
        self._reset_history()
        root = self.root
        activities = root.dumps.bugs_activity
        activities_fields = activities.colnames
        creations = root.dumps.bugs
        creations_fields = creations.colnames
        field_map = {
            3: 'component',
            8: 'status',
            10: 'keywords',
            11: 'resolution',
            12: 'severity',
            13: 'priority',
            14: 'subcomponent',
            53: 'program',
            42: 'flags',
        }
        # if True changing component resets priority
        self.component_reset_status = True

        n = len(activities)
        i = 0
        creation_ids = list(frozenset([bug['id'] for bug in creations]))
        n_bugs = len(creation_ids)
        print "Found %s bugs" % n_bugs
        creation_ids.sort()
        lowercase_fields = frozenset(field_map.values())
        rem = 'removed'
        ch_flds = 'changed_fields'
        prio = 'priority'
        stat = 'status'
        processed_bugs = 0
        for bug in creations:
            # The theory is that each given field has its status until it's changed
            group = {}
            bug_id = bug['id']
            prc = float(processed_bugs)/float(n_bugs) * 100
            print 'Processed %.2f%% bugs' % prc,
            print '\r',
            # Map bug fields to field names
            bug = dict(zip(creations_fields, bug[:]))
            # TODO: use datetime objects
            ts = int(bug['ts'])
            bug['ts'] = ts
            # TODO: do this at dump stage
            will_change = {}
            for field in lowercase_fields:
                if bug.has_key(field):
                    bug[field] = bug[field].lower()
                else:
                    bug[field] = ''
                if field in self.additive_fields:
                    will_change[field] = {}
                else:
                    will_change.setdefault(field, int(ts)+1)
            # Buffer creation coordinate
            group[ts] = bug.copy()
            next_ts = ts
            prev_event = {}
                
            # Look for matching activity coordinates
            while i < n:
                # Map coordinate fields to field names
                coord_id = activities[i]['bug_id']
                if not coord_id == bug_id:
                    # Reached end of coordinates for current bug
                    # Go to next bug with the same coordinate until ids match
                    break
                if not coord_id in creation_ids:
                    # Skip orphan coordinates (should never happen)
                    logging.warning('Orphan coordinate: %s' % (coord_id,))
                    i += 1
                    continue
                coord = dict(zip(activities_fields, activities[i]))
                ts = int(coord['ts'])
                # Merge coordinate with previous state
                try:
                    # field is the field that changed
                    # all the rest will stay the same 
                    field = field_map[int(coord['fieldid'])]
                except ValueError, e:
                    pp(coord['fieldid'])
                added = coord['added'].lower()
                removed = coord[rem].lower()
                if not added and not removed:
                    # Skipping stuff that didn't have activity (flags handling)
                    i += 1
                    continue
                # this keeps a record of last ts when some field got changed
                # it is there to cope with changes that don't get written to the activities
                dts= datetime.fromtimestamp(ts)
                if field in lowercase_fields:
                    if field in self.additive_fields:
                        removed = ','.join([x.strip() for x in removed.split(',')])
                        added = ','.join([x.strip() for x in added.split(',')])
                    #print '[%s] field: %s, added: %s, removed: %s' % (datetime.fromtimestamp(ts), field, added, removed)
                    if ts != next_ts:
                        next_event = group[next_ts]
                        # this is a new coordinate in time
                        # copy the stuff from before (since we go backward, before is next)
                        cur_event = group[ts] = next_event.copy()
                        for ks in prev_event.keys():
                            if cur_event[ks] != prev_event[ks]:
                                # set what we know are the next keys
                                self.update_this(ks, prev_event[ks], cur_event, will_change, ts)
                            del prev_event[ks]
                        # set new datetime
                        cur_event['ts'] = ts
                        # set field that changed/was added
                        if cur_event[field] != added:
                            # we need to set it also for later as it is not like this
                            self.set_next_events(ts, group, field, added, will_change)
                        # initialize (new event) and record what changed
                        cur_event[ch_flds] = field
                        cur_event[rem] = '%s>>%s' % (field, removed)
                        # initialize next event dict
                        # remember what was the next_ts
                        next_ts = ts
                    else:
                        # we have already this time coordinate recorded
                        # added should be already there since we are going
                        # in reverse order
                        cur_event = group[ts]
                        if not added.lower() == cur_event[field].lower():
                            # WTF we didn't know this status already
                            # Let's go through all the existing bugs 
                            # that have ts higher than this and set it
                            self.set_next_events(ts, group, field, added, will_change)
                        changed_fields = []
                        if cur_event.has_key(ch_flds):
                            changed_fields = cur_event[ch_flds].split('||')
                        changed_fields.append(field)
                        cur_event[ch_flds] = '||'.join(changed_fields)
                        rem_fields = []
                        if cur_event.has_key(rem):
                            rem_fields = cur_event[rem].split('||')
                        rem_fields.append('%s>>%s' % (field, removed))
                        cur_event[rem] = '||'.join(rem_fields)
                        if field == 'component' and self.component_reset_status \
                        and prio not in changed_fields \
                        and cur_event['status'] not in self.closed_states:
                            changed_fields.append('priority')
                            cur_event[ch_flds] = '||'.join(changed_fields)
                            rem_fields.append('priority>>%s' % (cur_event[prio]))
                            cur_event[rem] = '||'.join(rem_fields)
                            prev_event[prio] = ['', cur_event[prio]]
                            will_change[prio] = ts
                            cur_event[prio] = 'unspecified'
                    # and remember that prev_event has field as removed 
                    prev_event[field] = [added,removed]
                i += 1
            # Ok now we are out of bugs_activity it means we are at the bug creation
            ts = int(bug['creation_ts'])
            first_event = group[ts] = group[next_ts].copy()
            for ks in prev_event.keys():
                if ks in self.additive_fields and ts == next_ts:
                    # This happened at the same time of the creation
                    # So we need to actually add this to the first_event, not remove it
                    #print "creation: %s , next_ts: %s" % (datetime.fromtimestamp(ts), datetime.fromtimestamp(next_ts))
                    #print "%s: %s" % (ks,prev_event[ks])
                    prev_event[ks].reverse()
                # set what we know are the next keys
                self.update_this(ks, prev_event[ks], first_event, will_change, ts)
            first_event[ch_flds] = ''
            first_event[rem] = ''
            first_event['ts'] = ts
            bug_history = []
            gk = group.keys()
            gk.sort(cmp=lambda x,y:int(y)-int(x))
            for timestamp in gk:
                bug_history.append(group[timestamp])
            # Push the complete buffered group of bug coordinates
            self.commit(bug_history)
        # Clean up
        # TODO: don't use cache.file?
            processed_bugs += 1
        self.cache.file.flush()
    def commit(self, group):
        """
        
        """
        if self.cache.mode == 'r':
            return
        row = self.root.history.row
        for bug in group:
            for k, v in bug.iteritems():
                row[k] = v
            row.append()
    
#    def reset(self):
#        if 'history' in root:
#            root.history.remove()
#        if not 'history' in root:
#            file.createTable('/', 'history', Coord, 'Bug coordinates')
