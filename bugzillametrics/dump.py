"""

"""
import logging

import sqlalchemy as sa


class Dump(object):
    """
    
    """
    
    def __init__(self, config, cache):
        """
        
        """
        self.config = config
        self.cache = cache
        bugs_restrictions = ba_restrictions = ''
        if self.config.run.restrict:
            r = self.config.run.restrict.split(':')
            if len(r) == 2:
                restr = 'BETWEEN %s AND %s' % (r[0],r[1])
            else:
                restr = 'in (%s)' % self.config.run.restrict
            bugs_restrictions = "AND id %s" % restr
            ba_restrictions = "AND a.bug_id %s" % restr
        
        # TODO: use classes
        self._tables = {
            'bugs': ['bug_id AS id', 'keywords', 'bug_status AS status', 'resolution',
                'bug_severity AS severity', 'UNIX_TIMESTAMP(delta_ts) AS ts', 'priority',
                'UNIX_TIMESTAMP(creation_ts) AS creation_ts'],
            'bugs_activity': ['a.bug_id AS bug_id', 'fieldid', 'added', 'removed', 'UNIX_TIMESTAMP(bug_when) AS ts', 'c.name AS program'],
            'classifications': ['id', 'name'],
        }

        self._tables_addons = {
            'bugs': {
                'prefix': ['SELECT * FROM ('],
                'cols': ['pd.name AS component', 'co.name AS subcomponent', 'c.name AS program'],
                'postfix': ['''LEFT JOIN products AS pd ON (a.product_id = pd.id)
                LEFT JOIN classifications AS c ON (pd.classification_id = c.id)
                LEFT JOIN components AS co ON (a.component_id = co.id)
                ORDER BY id, ts DESC''', ''') AS q WHERE program = "Harmattan" %s ''' % bugs_restrictions],
            },
            'bugs_activity': {
                'postfix': ['''
                LEFT JOIN bugs b ON a.bug_id = b.bug_id
                LEFT JOIN products AS pd ON b.product_id = pd.id
                LEFT JOIN classifications AS c ON (pd.classification_id = c.id)
                WHERE c.name = "Harmattan" AND a.attach_id IS NULL AND
                fieldid IN (53, 3, 14, 8, 10, 11, 12, 13, 42) %s
                ORDER BY bug_id, ts DESC''' % ba_restrictions],
            },
        }

        args = dict(config.parser.items('connection'))
        url = sa.engine.url.URL('mysql', **args)
        engine = sa.create_engine(url)
        engine.echo = config.run.debug_sql
        self._engine = engine
        
    def run(self):
        """
        
        """
        logging.info('Stage: dump')
        template = """%(prefix)s SELECT %(cols)s FROM %(table)s a %(postfix)s"""
        
        for name, cols in self._tables.iteritems():
            prefix = []
            postfix = []
                
            if name in self._tables_addons:
                addons = self._tables_addons
                if 'cols' in addons[name]:
                    cols.extend(addons[name]['cols'])
                if 'postfix' in addons[name]:
                    postfix.extend(addons[name]['postfix'])
                if 'prefix' in addons[name]:
                    prefix.extend(addons[name]['prefix'])
            sql = sa.text(template % dict(cols=', '.join(cols), table=name,
                postfix=' '.join(postfix), prefix=' '.join(prefix)))
            self._dump_query(self._engine.execute(sql), cols, name)
    
    def _dump_query(self, query, cols, table_name):
        """
        
        """
        cache = self.cache
        _cols = [x.split(' ')[-1] for x in cols]
        table = cache.make_table(_cols, table_name, cache.dumps)
        row = table.row
        for item in query:
            for i, value in enumerate(item):
                row[_cols[i]] = value
            row.append()
        table.flush()
