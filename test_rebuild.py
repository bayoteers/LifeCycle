import tables
from datetime import datetime

debug = False

t = tables.openFile('/var/cache/bugzillametrics/cache.h5')
#for k in t.root.dumps.flags:
#    print 'bug_id:%s\nflags:%s\n' % (k['bug_id'],k['name'])
hist = t.root.history
hist_fields = hist.colnames
i = 0
n = len(hist)


bug_ids = list(frozenset([bug['id'] for bug in hist]))
bug_ids.sort()
#bug_ids = [250684]
print "Found %s bugs" % len(bug_ids)
chgd = 'changed_fields'
rem = 'removed'
additive_fields = ['flags', 'keywords']
broken_bugs = 0
broken_bugids = []
for bug_id in bug_ids:
    #print "========= BUG NUMBER %s ============" % bug_id
    bug_history = []
    #print '%s' % bug_id,end='\r'
    bug = []
    go = True
    while i < n and go:    
        #print hist[i]['id'], bug_id, end='\r'
        #print "%s" % i, end='\r'
        if hist[i]['id'] == bug_id:
            coord = dict(zip(hist_fields, hist[i])) 
            bug_history.append(coord)
        elif int(hist[i]['id']) > bug_id:
            go = False
        i += 1 
    bug_history.sort(cmp=lambda x,y:int(x['ts'])-int(y['ts']))
    #print frozenset([bug['id'] for bug in bug_history])
    broken = False
    # trying to rebuild the history
    for event in bug_history:
        if bug:
            # check what field changed
            changed = event[chgd].split('||')
            mutable_fields = [rem, chgd, 'ts']
            for field in changed:
                if field is not '':
                    curbroken = False
                    if event[rem] != '':
                        removed = dict([x.split('>>') for x in event[rem].split('||')])
                        if field not in additive_fields:
                            if removed[field] != bug[field]:
                                if debug:
                                    print "removed: %s  is different than %s" % (removed[field],bug[field])
                                broken = broken or True
                                curbroken = True
                        elif removed[field]:
                            bug_fields = set(bug[field].split(','))
                            rem_fields = set(removed[field].split(','))
                            if not rem_fields.issubset(bug_fields):
                                #pass
                                if debug:
                                    print "Rem fields is not a subset of fields", bug_fields, rem_fields
                                broken = broken or True
                                curbroken = True
                        else:
                            # There was a field changed but nothing was removed
                            # Make sure that really nothing was removed
                            cur_fields = set(event[field].split(','))
                            bug_fields = set(bug[field].split(','))
                            if not bug_fields.issubset(cur_fields) and bug_fields != set(['']):
                                if debug:
                                    print bug_fields
                                    print "Something got removed from %s" % field
                                    print datetime.fromtimestamp(bug['ts']),bug
                                    print "=========================="
                                    print datetime.fromtimestamp(event['ts']),event
                                broken = broken or True
                                curbroken = True
            # Now making sure that all the rest didn't change
            if changed:
                mutable_fields.extend(changed)
            for field in hist_fields:
                if field not in mutable_fields:
                    if bug[field] != event[field]:
                        if debug:
                            print "UPS... %s changed but it should have remained the same" % field
                            print datetime.fromtimestamp(bug['ts']),bug
                            print "=========================="
                            print datetime.fromtimestamp(event['ts']),event
                        broken = broken or True
                        curbroken = True
            # TODO: Last check would be to scan all the events in bugs_activity and make sure that 
            #       added and removed are present in history... but it's too long now...
            if curbroken and debug:
                print '\n\n'
                print "Bug #%s history is broken" % str(bug_id)
                print datetime.fromtimestamp(event['ts']),event

        bug = event.copy()
        #print datetime.fromtimestamp(bug['ts'])
        #print bug
        #print '========================='
    if broken:
        print 'history is not consistent for bug %s' % bug_id
        broken_bugs += 1
        broken_bugids.append(str(bug_id))


prc = float(broken_bugs)/float(len(bug_ids))*100

print 'Found %s broken bugs (%.2f%%)' % (broken_bugs, prc)
if broken_bugids:
    print 'Debug using --restrict=%s' % (','.join(broken_bugids))
